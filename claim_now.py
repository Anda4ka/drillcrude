"""Quick script to claim CRUDE rewards for drilled epochs."""
import asyncio, json, os, sys
from pathlib import Path
from dotenv import load_dotenv
import aiohttp

load_dotenv()

BANKR_API_KEY = os.getenv("BANKR_API_KEY", "")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "https://coordinator-production-38c0.up.railway.app")
DRILLER_ADDRESS = os.getenv("DRILLER_ADDRESS", "")
STATE_FILE = Path("crude_driller_state.json")


async def main():
    # Load state
    if not STATE_FILE.exists():
        print("No state file found")
        return
    state = json.loads(STATE_FILE.read_text())
    drilled_epochs = state.get("drilled_epochs", [])
    print(f"Drilled epochs in state: {drilled_epochs}")

    if not drilled_epochs:
        print("No epochs to claim!")
        return

    async with aiohttp.ClientSession() as session:
        # Get current epoch info
        async with session.get(f"{COORDINATOR_URL}/v1/epoch") as resp:
            epoch_data = await resp.json()
        current_epoch = epoch_data.get("epochId")
        prev_epoch = epoch_data.get("prevEpochId")
        print(f"Current epoch: {current_epoch}, Previous epoch: {prev_epoch}")

        # Try claiming each drilled epoch (skip current — can't claim active epoch)
        claimable = [e for e in drilled_epochs if e != current_epoch]
        if not claimable:
            print(f"All drilled epochs ({drilled_epochs}) are the current epoch — nothing to claim yet.")
            return

        print(f"Attempting to claim epochs: {claimable}")

        for epoch in claimable:
            print(f"\n--- Claiming epoch {epoch} ---")
            # Get claim calldata
            async with session.get(
                f"{COORDINATOR_URL}/v1/claim-calldata?epochs={epoch}"
            ) as resp:
                claim_data = await resp.json()

            print(f"Claim data: {json.dumps(claim_data, indent=2)[:500]}")

            tx = claim_data.get("transaction", {})
            if not tx:
                print(f"No transaction data for epoch {epoch} — maybe no rewards or already claimed?")
                continue

            # Submit via Bankr
            print(f"Submitting claim tx to Bankr...")
            async with session.post(
                "https://api.bankr.bot/agent/submit",
                json={
                    "transaction": {
                        "to": tx["to"],
                        "chainId": tx["chainId"],
                        "value": tx.get("value", "0"),
                        "data": tx["data"]
                    },
                    "description": f"Claim CRUDE epoch {epoch}",
                    "waitForConfirmation": True
                },
                headers={"Content-Type": "application/json", "X-API-Key": BANKR_API_KEY}
            ) as resp:
                result = await resp.json()

            if result.get("success"):
                tx_hash = result.get("transactionHash", "?")
                print(f"OK Epoch {epoch} claimed! TX: {tx_hash}")
                # Remove from state
                drilled_epochs.remove(epoch)
                state["drilled_epochs"] = drilled_epochs
                STATE_FILE.write_text(json.dumps(state, indent=2))
                print(f"State updated, remaining epochs: {drilled_epochs}")
            else:
                print(f"FAIL Claim failed for epoch {epoch}: {result.get('error', result)}")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
