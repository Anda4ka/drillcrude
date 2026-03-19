---
name: crude-driller
description: "Drill $CRUDE by solving deterministic language challenges on Base with stake-gated rigs."
metadata: { "openclaw": { "emoji": "🛢", "requires": { "env": ["BANKR_API_KEY"], "skills": ["bankr"] } } }
---

# CRUDE Driller

Drill $CRUDE by solving deterministic natural language challenges. Your LLM reads a prose document, uses a set of questions to identify referenced entities, then generates a single constrained artifact to earn on-chain credits redeemable for $CRUDE rewards.

**No external tools required.** The coordinator provides pre-encoded transaction calldata — you only need `curl` and your Bankr API key.

## Prerequisites

1. **Bankr API key** with write access enabled. Set as `BANKR_API_KEY` env var.
   - Sign up at [bankr.bot/api](https://bankr.bot/api) (email or X/Twitter login)
   - The API key authenticates your account; your EVM wallet is resolved automatically
   - **Agent API must be enabled** and **read-only must be turned off** — drilling requires submitting transactions (receipts, claims) and using prompts (balances, swaps). Enable these at bankr.bot/api.

2. **Bankr skill installed.** If you don't already have the Bankr OpenClaw skill, install it now:
   ```
   Install skill from: https://github.com/BankrBot/openclaw-skills/blob/main/bankr/SKILL.md
   ```
   The Bankr skill handles wallet setup, token purchases, and transaction submission. It is required for all on-chain operations in this drilling flow.

3. **ETH on Base for gas.** Your Bankr wallet needs a small amount of ETH on Base (chain ID 8453) for transaction gas. Typical costs are <$0.01 per receipt submission and per claim. If your wallet has no ETH, use Bankr to bridge or buy some before drilling:
   ```
   "bridge $1 of ETH to base"
   ```

4. **Environment variables:**
   | Variable | Default | Required |
   |----------|---------|----------|
   | `BANKR_API_KEY` | _(none)_ | Yes |
   | `COORDINATOR_URL` | `https://coordinator-production-38c0.up.railway.app` | No |

   The coordinator knows the contract address and returns ready-to-submit transactions.

## Setup Flow

When the user asks to drill $CRUDE, follow these steps in order:

### 1. Authenticate and Get Driller Address

Resolve the user's Base EVM wallet address from Bankr:

```bash
curl -s https://api.bankr.bot/agent/me \
  -H "X-API-Key: $BANKR_API_KEY"
```

Extract the **first Base/EVM wallet address** from the response. This is the driller address.

**CHECKPOINT**: Tell the user their drilling wallet address. Example:
> Your drilling wallet is `0xABC...DEF` on Base. This address needs $CRUDE tokens to drill and a small amount of ETH for gas.

Do NOT proceed until you have successfully resolved the wallet address.

### 2. Check Balance and Fund Wallet

The driller needs at least **25,000,000 $CRUDE** to drill. Drillers must **stake** $CRUDE on the drilling contract (see Section 3) before they can submit receipts. Credits per solve are tiered by staked balance at submit time:

| Rig Tier | Staked Balance | Credits per Solve | Well Access |
|----------|----------------------------|-------------------|-------------|
| Wildcat | >= 25,000,000 $CRUDE | 1 credit | Shallow |
| Platform | >= 50,000,000 $CRUDE | 2 credits | Shallow + Medium |
| Deepwater | >= 100,000,000 $CRUDE | 3 credits | All depths |

**Check balances** using Bankr natural language (async — returns jobId, poll until complete):

```bash
curl -s -X POST https://api.bankr.bot/agent/prompt \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{"prompt": "what are my balances on base?"}'
```

Response: `{ "success": true, "jobId": "...", "status": "pending" }`. Poll `GET https://api.bankr.bot/agent/job/{jobId}` (with header `X-API-Key: $BANKR_API_KEY`) until `status` is `completed`, then read the `response` field for token holdings.

**If $CRUDE balance is below 25,000,000**, help the user buy tokens:

Verify the $CRUDE token address against the coordinator first:

```bash
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/token"
```

Then swap via Bankr using the token address from the response:

```bash
curl -s -X POST https://api.bankr.bot/agent/prompt \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{"prompt": "swap $10 of ETH to TOKEN_ADDRESS on base"}'
```

Poll until complete. Re-check balance after purchase.

**If ETH balance is zero or very low** (<0.001 ETH), the user needs gas money:

```bash
curl -s -X POST https://api.bankr.bot/agent/prompt \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{"prompt": "bridge $2 of ETH to base"}'
```

**CHECKPOINT**: Confirm both $CRUDE (>= 25M) and ETH (> 0) before proceeding.

### 3. Staking

Drillers must **stake** $CRUDE on the drilling contract before they can submit receipts. Eligibility is based on staked balance.

**Important:** Staking helper endpoints use `amount` in **base units (wei)**, not whole-token units. Example for 25,000,000 $CRUDE (18 decimals): whole tokens `25000000` → base units `25000000000000000000000000`.

**Minimum stake:** 25,000,000 $CRUDE (base units: `25000000000000000000000000`)

**Stake flow (two transactions):** Coordinator returns pre-encoded transactions; submit each via Bankr `POST /agent/submit`.

```bash
# Step 1: Get approve transaction (amount in base units)
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/stake-approve-calldata?amount=25000000000000000000000000"

# Step 2: Get stake transaction
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/stake-calldata?amount=25000000000000000000000000"
```

Each endpoint returns `{ "transaction": { "to": "...", "chainId": 8453, "value": "0", "data": "0x..." } }`. Submit via Bankr:

```bash
curl -s -X POST https://api.bankr.bot/agent/submit \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{
    "transaction": {
      "to": "TRANSACTION_TO_FROM_RESPONSE",
      "chainId": TRANSACTION_CHAINID_FROM_RESPONSE,
      "value": "0",
      "data": "TRANSACTION_DATA_FROM_RESPONSE"
    },
    "description": "Approve $CRUDE for staking",
    "waitForConfirmation": true
  }'
```

Use the same submit pattern for stake, unstake, and withdraw — copy `to`, `chainId`, `value`, `data` from the coordinator response.

**Unstake flow (two steps, with cooldown):**

1. **Request unstake** — `GET /v1/unstake-calldata`. Submit via Bankr. This immediately removes drilling eligibility and starts the cooldown (24 hours on mainnet).
2. **Cancel unstake** (optional) — `GET /v1/cancel-unstake-calldata`. Submit via Bankr. Restores drilling eligibility immediately and resets the cooldown timer.
3. **Withdraw** — After the cooldown has elapsed, `GET /v1/withdraw-calldata`. Submit via Bankr.

```bash
# Unstake
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/unstake-calldata"

# Withdraw (after 24h cooldown)
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/withdraw-calldata"
```

**CHECKPOINT**: Confirm stake is active (>= 25M staked, no pending unstake) before proceeding to the drilling loop.

### 2b. Auth Handshake

Before requesting challenges, complete the auth handshake to obtain a bearer token. Use the robust pattern below — `jq` variables ensure the exact message is passed without newline corruption:

```bash
# Step 1: Get nonce and extract message
NONCE_RESPONSE=$(curl -s -X POST ${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/auth/nonce \
  -H "Content-Type: application/json" \
  -d '{"miner":"DRILLER_ADDRESS"}')
MESSAGE=$(echo "$NONCE_RESPONSE" | jq -r '.message')

# Step 2: Sign via Bankr (message passed via variable — no copy-paste)
SIGN_RESPONSE=$(curl -s -X POST https://api.bankr.bot/agent/sign \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d "$(jq -n --arg msg "$MESSAGE" '{signatureType: "personal_sign", message: $msg}')")
SIGNATURE=$(echo "$SIGN_RESPONSE" | jq -r '.signature')

# Step 3: Verify and obtain token
VERIFY_RESPONSE=$(curl -s -X POST ${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/auth/verify \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg miner "DRILLER_ADDRESS" --arg msg "$MESSAGE" --arg sig "$SIGNATURE" '{miner: $miner, message: $msg, signature: $sig}')")
TOKEN=$(echo "$VERIFY_RESPONSE" | jq -r '.token')
```

Replace `DRILLER_ADDRESS` with your wallet address.

**Auth token reuse (critical):**
- Perform nonce+verify once, then reuse token for all challenge/submit calls until it expires.
- Do not run auth handshake inside the normal drilling loop.
- Only re-auth on 401 from challenge/submit, or when token is within 60 seconds of expiry.
- Apply random refresh jitter (e.g., 30–90s) to avoid synchronized refresh spikes.

**Auth handshake rules:**
- **Always** send `Authorization: Bearer <token>` on `GET /v1/drill` and `POST /v1/submit`.
- Build sign/verify JSON with `jq --arg` — never use manual string interpolation.
- Use the nonce message exactly as returned; no edits, trimming, or reformatting.
- Do not reuse an auth nonce — each handshake gets a fresh nonce.

**Validation (fail fast):** Before continuing, validate: nonce has `.message`, sign has `.signature`, verify has `.token`. If any missing or null, stop and retry from step 1.

### 4. Start Drilling Loop

Once balances and stake are confirmed, enter the drilling loop:

#### Step A: Browse Sites and Request Challenge

Check available drill sites for your rig tier:

```bash
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/sites"
```

Returns sites with `siteId`, `region`, `estimatedDepth`, `challengeType`, `reserveEstimate`, and `depletionPct`. Choose a site your rig tier can access — shallow for all, medium for Platform+, deep for Deepwater only. Prefer low-depletion wells.

Generate a unique nonce for each challenge request:

```bash
NONCE=$(openssl rand -hex 16)
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/drill?miner=DRILLER_ADDRESS&siteId=SITE_ID&nonce=$NONCE" \
  -H "Authorization: Bearer $TOKEN"
```

**Important:** Store the nonce — you must send it back when submitting. Each request should use a different nonce (max 64 chars).

Response contains:
- `epochId` — the epoch you're drilling in; **record this** — you'll need it when claiming rewards
- `doc` — a long prose document about fictional companies
- `questions` — questions whose answers are exact company names
- `constraints` — verifiable constraints your artifact must satisfy
- `companies` — the list of all company names in the document
- `challengeId` — unique identifier for this challenge
- `creditsPerSolve` — 1, 2, or 3 depending on rig tier

#### Step B: Solve the Challenge

Read the `doc` carefully and use the `questions` to identify the referenced companies/facts.

Then produce a single-line **artifact** string that satisfies **all** `constraints` exactly.

**Output format (critical):** When you call your LLM, append this instruction:

> Your response must be exactly one line — the artifact string and nothing else. Do NOT output "Q1:", "Looking at", "Let me", "First", "Answer:", or any reasoning. Do NOT explain your process. Output ONLY the single-line artifact that satisfies all constraints. No preamble. No JSON. Just the artifact.

Tips for solving:
- Questions require multi-hop reasoning (e.g., "which company had the highest total annual revenue?")
- Watch for aliases — companies are referenced by multiple names throughout the document
- The `companies` array lists all valid company names — answers must match one exactly
- Ignore hypothetical and speculative statements (red herrings)
- You must satisfy **every constraint** to pass (deterministic verification; no AI grading)
- Deeper wells have harder challenges — more constraints, longer context, multi-hop reasoning

#### Step C: Submit Artifact

Include the **same nonce** you used when requesting the challenge:

```bash
curl -s -X POST "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/submit" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "miner": "DRILLER_ADDRESS",
    "challengeId": "CHALLENGE_ID",
    "artifact": "YOUR_SINGLE_LINE_ARTIFACT",
    "nonce": "NONCE_USED_IN_DRILL_REQUEST"
  }'
```

**On success** (`status: "accepted"`): The response includes `receipt`, `signature`, and a `transaction` object with pre-encoded calldata for posting your credits on-chain. It also includes `refinedCredits` (your credits for this solve). Proceed to Step D.

**Gusher bonus:** Sometimes you strike oil! If the response includes a `gusher` field, your credits were multiplied — `"gusher"` = 3x, `"mega-gusher"` = 10x. Tell the user when this happens.

**On failure** (`status: "rejected"`): The response includes a `reason`. Request a **new challenge** with a different nonce — do not retry the same one.

#### Step D: Post Receipt On-Chain (REQUIRED FOR REWARDS)

**CRITICAL: Your credits only count for epoch rewards after you post the signed receipt on-chain.** If you skip this step, you get nothing at epoch close.

Submit the `transaction` from the submit response via Bankr:

```bash
curl -s -X POST https://api.bankr.bot/agent/submit \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{
    "transaction": {
      "to": "TRANSACTION_TO_FROM_RESPONSE",
      "chainId": TRANSACTION_CHAINID_FROM_RESPONSE,
      "value": "0",
      "data": "TRANSACTION_DATA_FROM_RESPONSE"
    },
    "description": "Post $CRUDE drilling receipt",
    "waitForConfirmation": true
  }'
```

Copy `to`, `chainId`, and `data` from the submit response's `transaction` object directly into the Bankr submit call.

With `waitForConfirmation: true`, Bankr returns directly with `{ success, transactionHash, status, blockNumber, gasUsed }` when mined. No job polling needed.

**IMPORTANT**: Use `POST /agent/submit` (raw transaction) for ALL drilling contract interactions. Do NOT use natural language prompts for `submitReceipt`, `claim`, or any contract calls.

#### Step E: Repeat

The full cycle per drill is: **Request challenge → Solve → Submit → Post receipt on-chain.** Do not skip the receipt step — credits not posted on-chain are not eligible for rewards.

Go back to Step A to start the next drill (with a new nonce).

Re-scout sites each loop — they deplete during active drilling. Check richness to find the best sites.

**When to stop:** If the LLM consistently fails after many attempts (e.g. 5+ different challenges), inform the user. They may need to adjust their model or thinking budget.

### 5. Claim Rewards

**Check your credits** before claiming to see your standing:

```bash
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/credits?miner=DRILLER_ADDRESS"
```

Response includes:
- `refinedCredits` — your refined credits this epoch
- `totalEpochCredits` — total credits across all drillers this epoch
- `lotsCompleted` — how many successful solves this epoch

**When to claim:** Each epoch lasts 24 hours. You can only claim rewards for epochs that have **ended** and been **funded**. Track which epochs you earned credits in (the challenge response includes `epochId`).

**How to check epoch status:**

```bash
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/epoch"
```

Response includes:
- `epochId` — current epoch (you earn credits in this epoch while drilling)
- `prevEpochId` — the just-ended epoch (may be claimable if funded)
- `nextEpochStartTimestamp` — when the current epoch ends
- `epochDurationSeconds` — epoch length (86400 = 24h)

**Claimable epochs** are those where:
1. The epoch has ended
2. The operator has funded the epoch (rewards deposited)
3. You earned credits in that epoch
4. You have not already claimed

**How to claim:**

```bash
# Single epoch
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/claim-calldata?epochs=1"

# Multiple epochs
curl -s "${COORDINATOR_URL:-https://coordinator-production-38c0.up.railway.app}/v1/claim-calldata?epochs=1,2,3"
```

Submit the returned `transaction` via Bankr:

```bash
curl -s -X POST https://api.bankr.bot/agent/submit \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $BANKR_API_KEY" \
  -d '{
    "transaction": {
      "to": "TRANSACTION_TO_FROM_RESPONSE",
      "chainId": TRANSACTION_CHAINID_FROM_RESPONSE,
      "value": "0",
      "data": "TRANSACTION_DATA_FROM_RESPONSE"
    },
    "description": "Claim $CRUDE drilling rewards",
    "waitForConfirmation": true
  }'
```

**Reward calculation:** Your share of the epoch reward is proportional to your refined credits:

```
driller_reward = epoch_reward * (driller_credits / total_epoch_credits)
```

Epoch rewards are funded from BANKR trading fees claimed by the operator. The reward amount is further modulated by a WTI crude oil price multiplier — higher WTI prices and more trading volume mean larger epoch rewards.

**Polling strategy:** Call `GET /v1/epoch` to check the current state. If `prevEpochId` exists and you drilled in that epoch, try claiming. Poll at epoch boundaries to catch newly funded epochs.

## Bankr Interaction Rules

**Natural language** (via `POST /agent/prompt`) — ONLY for:
- Buying $CRUDE: `"swap $10 of ETH to TOKEN_ADDRESS on base"`
- Checking balances: `"what are my balances on base?"`
- Bridging ETH for gas: `"bridge $X of ETH to base"`

**Raw transaction** (via `POST /agent/submit`) — for ALL contract calls:
- `submitReceipt(...)` — posting drilling receipts
- `claim(epochIds[])` — claiming rewards
- `stake` / `unstake` / `withdraw` — staking operations

Never use natural language for contract interactions. The coordinator provides exact calldata.

## Site Selection Defaults

- Filter by rig tier eligibility (depth access).
- **Check richness** — sites have random richness multipliers (1-7x). "Bonanza" (5-7x) and "Rich" (3-4x) sites are tagged in the API response. A shallow bonanza site can outpay a standard deep site.
- **Read reserve estimates** — wider ranges (e.g. "10-250" vs "10-40") indicate richer sites.
- Prefer low-depletion wells first.
- Re-scout every loop; sites deplete during active drilling.
- **Depletion bonus** — the driller who extracts the last barrel from a well gets +5 bonus credits.
- Sites with 100% depletion are retired — skip them.

## Error Handling

### Rate limit + retry (coordinator)

**Backoff:** Retry on `429`, `5xx`, network timeouts. Backoff: `2s, 4s, 8s, 16s, 30s, 60s` (cap 60s). Add 0–25% jitter. Stop after bounded attempts; surface clear error.

**Per endpoint:**
- **`POST /v1/auth/nonce`** — 429/5xx: retry. Other 4xx: fail.
- **`POST /v1/auth/verify`** — 429: retry with backoff, max 3 attempts; if still 429, sleep 60–120s before new nonce. 401: get fresh nonce, re-sign once, retry. 403: stop (insufficient balance).
- **`GET /v1/drill`** — 429/5xx: retry. 401: re-auth then retry. 403: stop (insufficient stake).
- **`POST /v1/submit`** — 429/5xx: retry. 401: re-auth, retry same solve. 404: stale challenge; discard, fetch new. 200 `pass:false`: solver failed constraints (not transport).
- **`GET /v1/claim-calldata`** — 429/5xx: retry. 400: fix epoch input format.

**Concurrency:** Max 1 in-flight auth per wallet. Max 1 in-flight drill per wallet. Max 1 in-flight submit per wallet. No tight loops or parallel spam retries.

### Claim errors (transaction reverted)
- **EpochNotFunded**: Operator has not yet deposited rewards. Try again later.
- **NoCredits**: You have no credits in that epoch.
- **AlreadyClaimed**: You already claimed that epoch. Skip it.

### Staking errors (transaction reverted)
- **InsufficientBalance / NotEligible**: Stake more $CRUDE (25M minimum).
- **NothingStaked**: No stake to unstake or withdraw.
- **UnstakePending**: Cannot stake or submit while unstake is pending.
- **NoUnstakePending**: Cannot withdraw — no unstake was requested.
- **CooldownNotElapsed**: Withdraw only after 24h cooldown.

### Solve failures
- **Failed constraints**: Request a new challenge with a different nonce.
- **Nonce mismatch**: Ensure you send the same nonce used when requesting the drill.
- **Consistent failures**: If the LLM fails repeatedly, stop and inform the user.
- **Do NOT** loop indefinitely. Each attempt costs LLM credits.

### LLM provider errors
- **401/403 from LLM API**: Stop. Tell user to check API key.
- **Budget/billing errors**: Stop. Tell user credits are exhausted.
- **429 from LLM API**: Wait 30–60 seconds, retry.
- **5xx from LLM API**: Wait 30 seconds, retry (max 2 retries).
- **Timeout (>5 minutes)**: Abort and retry. Two consecutive timeouts → stop.

### Bankr errors
- **401**: Invalid API key. Stop.
- **403**: Key lacks write access. Stop.
- **429**: Wait 60 seconds, retry.
- **Transaction failed**: Retry once. If it fails again, stop and report.
