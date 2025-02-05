# Airdrop Scripts

## Calculate Airdrop Amounts
Adjusts CSV values to match a target total while maintaining proportions.

```bash
python3 calculateAirdrop.py input.csv output.csv 1000000
```

CSV format:
```csv
address,amount
0x123...,100
0x456...,200
```

## Send Airdrop (Hyperliquid)
Executes batch token transfers on Hyperliquid network.

```bash
python3 sendAirdrop.py private_key transfers.csv --token USDC [--mainnet]
```

CSV format: same as above

## How It Works

### Calculate Script
1. Reads amounts from input CSV
2. Applies factor: `target_total / current_total`
3. Writes adjusted amounts to output CSV

### Send Script
1. Batches transfers (1100 tx/batch)
2. Validates wallet balance & rate limits
3. Auto-retries failed transactions
4. Supports both networks (mainnet/testnet)