"""Claim CRUDE rewards + Royalty Pool — retries every 60s until success."""
import asyncio, json, os
from pathlib import Path
from dotenv import load_dotenv
import aiohttp

load_dotenv()

BANKR_API_KEY = os.getenv("BANKR_API_KEY", "")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "https://coordinator-production-38c0.up.railway.app")
DRILLER_ADDRESS = os.getenv("DRILLER_ADDRESS", "")
STATE_FILE = Path("crude_driller_state.json")


async def submit_tx(session, tx, description):
    """Submit transaction via Bankr. Returns (success, result)."""
    async with session.post(
        "https://api.bankr.bot/agent/submit",
        json={
            "transaction": {"to": tx["to"], "chainId": tx["chainId"], "value": tx.get("value", "0"), "data": tx["data"]},
            "description": description,
            "waitForConfirmation": True
        },
        headers={"Content-Type": "application/json", "X-API-Key": BANKR_API_KEY}
    ) as resp:
        return await resp.json()


async def try_claim_epoch(session, epoch):
    """Try to claim drilling rewards for one epoch. Returns True on success."""
    try:
        async with session.get(
            f"{COORDINATOR_URL}/v1/claim-calldata?epochs={epoch}&miner={DRILLER_ADDRESS}"
        ) as resp:
            claim_data = await resp.json()

        tx = claim_data.get("transaction", {})
        if not tx:
            print(f"  No tx data for epoch {epoch} — not funded yet or already claimed")
            return False

        result = await submit_tx(session, tx, f"Claim CRUDE epoch {epoch}")

        if result.get("success"):
            print(f"  ✅ Epoch {epoch} claimed! TX: {result.get('transactionHash', '?')}")
            return True
        else:
            err = result.get("error", str(result))
            if "reverted" in str(err):
                print(f"  ⏳ Epoch {epoch} not ready yet (reverted)")
            else:
                print(f"  ❌ Epoch {epoch} failed: {err}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


async def try_claim_royalty(session):
    """Try to claim Royalty Pool rewards. Returns True on success."""
    try:
        async with session.get(
            f"{COORDINATOR_URL}/v1/royalty-claim-calldata?depositor={DRILLER_ADDRESS}"
        ) as resp:
            data = await resp.json()

        if data.get("error"):
            print(f"  Royalty: {data['error']}")
            return False

        tx = data.get("transaction", {})
        if not tx:
            print(f"  Royalty: no tx data — nothing to claim")
            return False

        result = await submit_tx(session, tx, "Claim Royalty Pool rewards")

        if result.get("success"):
            print(f"  ✅ Royalty claimed! TX: {result.get('transactionHash', '?')}")
            return True
        else:
            err = result.get("error", str(result))
            if "reverted" in str(err):
                print(f"  ⏳ Royalty not ready yet (reverted)")
            else:
                print(f"  ❌ Royalty failed: {err}")
            return False
    except Exception as e:
        print(f"  ❌ Royalty error: {e}")
        return False


async def main():
    if not STATE_FILE.exists():
        print("No state file found")
        return

    async with aiohttp.ClientSession() as session:
        attempt = 0
        epoch_done = False
        royalty_done = False

        while True:
            attempt += 1

            # --- Epoch claims ---
            if not epoch_done:
                state = json.loads(STATE_FILE.read_text())
                drilled_epochs = state.get("drilled_epochs", [])

                try:
                    async with session.get(f"{COORDINATOR_URL}/v1/epoch") as resp:
                        epoch_data = await resp.json()
                    current_epoch = epoch_data.get("epochId")
                except Exception:
                    print(f"[{attempt}] Can't reach coordinator, retry in 60s...")
                    await asyncio.sleep(60)
                    continue

                claimable = [e for e in drilled_epochs if e != current_epoch]
                if claimable:
                    print(f"\n[{attempt}] Claiming epochs {claimable}...")
                    all_ok = True
                    for epoch in claimable[:]:
                        ok = await try_claim_epoch(session, epoch)
                        if ok:
                            drilled_epochs.remove(epoch)
                            state["drilled_epochs"] = drilled_epochs
                            STATE_FILE.write_text(json.dumps(state, indent=2))
                        else:
                            all_ok = False
                    if all_ok:
                        epoch_done = True
                else:
                    print(f"[{attempt}] No past epochs to claim")
                    epoch_done = True

            # --- Royalty Pool claim ---
            if not royalty_done:
                print(f"[{attempt}] Claiming Royalty Pool...")
                royalty_done = await try_claim_royalty(session)

            # --- Check if all done ---
            if epoch_done and royalty_done:
                print("\n🎉 All claimed! (epochs + royalty)")
                return

            what = []
            if not epoch_done:
                what.append("epochs")
            if not royalty_done:
                what.append("royalty")
            print(f"Waiting 60s... (pending: {', '.join(what)})")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
