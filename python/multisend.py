import csv
import time
import json
import argparse
import asyncio
import websockets
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from eth_account.signers.local import LocalAccount
from eth_account import Account
from eth_account.messages import encode_structured_data
from eth_utils import to_hex
import requests
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning, 
                       message='.*encode_structured_data.*')

class Config:
    HEADERS = {"Content-Type": "application/json"}
    API = {
        "testnet": {
            "rest": "https://api.hyperliquid-testnet.xyz",
            "ws": "wss://api.hyperliquid-testnet.xyz/ws"
        },
        "mainnet": {
            "rest": "https://api.hyperliquid.xyz",
            "ws": "wss://api.hyperliquid.xyz/ws"
        }
    }

class Logger:
    def __init__(self, log_dir: str = "logs"):
        # Setup log directory
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.log_file = self.log_dir / f"ws_responses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # Configure logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # File handler for debug logs
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Stream handler for summary (added later)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.INFO)
        self.stream_handler.setFormatter(logging.Formatter('%(message)s'))
        
        # Stats tracking
        self.stats = {
            "total": 0,
            "success": 0,
            "error": 0,
            "failed_txs": {},  # Map of nonce -> (address, amount)
            "start_time": datetime.now(),
            "end_time": None,
            "first_error": None
        }

    def log_ws_response(self, response: str, tx_map: Dict[int, Tuple[str, float]]) -> bool:
        try:
            raw = json.loads(response)
            data = raw.get("data", {})
            self.stats["total"] += 1
            
            tx_id = data.get("id")
            if not tx_id:
                self.logger.debug(f"Response without ID:\n{json.dumps(data, indent=2)}")
                return True
                
            # Check for both error type and error status/response
            response_data = data.get("response", {})
            has_error = (
                "error" in response_data.get("type", "") or
                response_data.get("payload", {}).get("status") == "err" or
                "Invalid nonce" in str(response_data.get("payload", {}).get("response", ""))
            )
            if has_error:
                self.stats["error"] += 1
                self.stats["failed_txs"][tx_id] = tx_map.get(tx_id)
                error_msg = f"Transaction {tx_id} failed:\n{json.dumps(data, indent=2)}"
                self.logger.debug(error_msg)
                if not self.stats["first_error"]:
                    self.stats["first_error"] = error_msg
                return False
            else:
                self.stats["success"] += 1
                self.logger.debug(f"Transaction {tx_id} succeeded:\n{json.dumps(data, indent=2)}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error processing response: {str(e)}\nResponse: {response}")
            return False
        
    def write_failed_txs(self):
        """Write failed transactions to a CSV file"""
        if not self.stats["failed_txs"]:
            return None
            
        failed_file = self.log_dir / f"failed_txs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(failed_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['address', 'amount'])
            for _, (address, amount) in self.stats["failed_txs"].items():
                writer.writerow([address, amount])
        return failed_file

    def write_summary(self):
        self.stats["end_time"] = datetime.now()
        duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        self.logger.addHandler(self.stream_handler)
        
        summary = f"""
=== Transfer Summary ===
Total Processed: {self.stats['total']}
Successful: {self.stats['success']}
Failed: {self.stats['error']}
Duration: {duration:.2f} seconds
Start: {self.stats['start_time']}
End: {self.stats['end_time']}
"""     
        self.logger.info(summary)

class TransferManager:
    def __init__(self, wallet: LocalAccount, is_mainnet: bool):
        self.wallet = wallet
        self.is_mainnet = is_mainnet
        self.api_base = Config.API["mainnet" if is_mainnet else "testnet"]
        self.BATCH_SIZE = 1100 #Rate limit
        self.SLEEP_DURATION = 60  # seconds

    
    async def process_batch(self, transfers: List[Dict], logger: Logger, batch_num: int) -> None:
        """Process a single batch of transfers"""
        print(f"\nProcessing batch {batch_num + 1}...")
        
        async with websockets.connect(self.api_base["ws"]) as ws:
            responses_received = 0
            expected_responses = len(transfers)

            # Background task to process responses
            async def process_responses():
                nonlocal responses_received
                try:
                    while responses_received < expected_responses:
                        response = await ws.recv()
                        logger.log_ws_response(response, tx_map)
                        responses_received += 1
                except Exception as e:
                    logger.logger.error(f"WebSocket error in batch {batch_num + 1}: {e}")
                    return False

            # Create mapping of nonce to (address, amount) for this batch
            tx_map = {
                tx['id']: (tx['request']['payload']['action']['destination'], 
                          float(tx['request']['payload']['action']['amount']))
                for tx in transfers
            }

            # Start background response processing
            response_task = asyncio.create_task(process_responses())
            
            try:
                print(f"Sending {len(transfers)} transactions in batch {batch_num + 1}...")
                start_send = time.time()
                sent_count = 0

                # Send all transfers in batch
                for transfer in transfers:
                    if response_task.done():
                        print(f"Stopping batch {batch_num + 1} due to error in response processing")
                        break
                    await ws.send(json.dumps(transfer))
                    sent_count += 1
                
                print(f"Sent {sent_count} transactions in {time.time() - start_send:.2f}s")
                print(f"Waiting for {expected_responses} responses...")
                
                # Wait for all responses
                while responses_received < expected_responses:
                    print(f"Received {responses_received}/{expected_responses} responses...", end='\r')
                    await asyncio.sleep(0.1)
                print(f"\nReceived all {expected_responses} responses for batch {batch_num + 1}")
                
            finally:
                if not response_task.done():
                    response_task.cancel()
                    try:
                        await response_task
                    except asyncio.CancelledError:
                        pass
    
    async def process_bulk_transfers(self, transfers: List[Dict], logger: Optional[Logger] = None) -> bool:
        """Process all transfers in batches of 1200 with 60s sleep between batches"""
        if not logger:
            logger = Logger()

        # Split transfers into batches
        batches = [transfers[i:i + self.BATCH_SIZE] for i in range(0, len(transfers), self.BATCH_SIZE)]
        print(f"\nSplit {len(transfers)} transfers into {len(batches)} batches of up to {self.BATCH_SIZE}")

        for i, batch in enumerate(batches):
            # Sleep between batches (except first one)
            if i > 0:
                print(f"\nSleeping {self.SLEEP_DURATION}s before next batch...")
                await asyncio.sleep(self.SLEEP_DURATION)
            
            await self.process_batch(batch, logger, i)

        # Handle failed transactions after all batches
        failure_rate = logger.stats["error"] / max(logger.stats["total"], 1)
        if 0 < failure_rate <= 0.05:  # 5% threshold
            print(f"\nRetrying {len(logger.stats['failed_txs'])} failed transactions...")
            failed_transfers = [
                self.create_signed_transfer(addr, str(amt), 
                    transfers[0]['request']['payload']['action']['token'],
                    int(time.time() * 1000) + i)
                for i, (_, (addr, amt)) in enumerate(logger.stats["failed_txs"].items())
            ]
            
            # Process retries in batches as well
            retry_batches = [failed_transfers[i:i + self.BATCH_SIZE] 
                           for i in range(0, len(failed_transfers), self.BATCH_SIZE)]
            
            for i, retry_batch in enumerate(retry_batches):
                if i > 0:
                    print(f"\nSleeping {self.SLEEP_DURATION}s before next retry batch...")
                    await asyncio.sleep(self.SLEEP_DURATION)
                await self.process_batch(retry_batch, logger, i)
        else:
            failed_file = logger.write_failed_txs()
            if failed_file:
                logger.logger.info(f"\nFailed transactions written to: {failed_file}")


    def create_signed_transfer(self, dest: str, amount: str, token: str, timestamp: int) -> Dict:
        """Create a single signed transfer"""
        action = {
            "destination": dest,
            "amount": amount,
            "token": token,
            "time": timestamp,
            "type": "spotSend",
            "signatureChainId": "0x66eee",
            "hyperliquidChain": "Mainnet" if self.is_mainnet else "Testnet"
        }
        
        data = {
            "domain": {
                "name": "HyperliquidSignTransaction",
                "version": "1",
                "chainId": 421614,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "types": {
                "HyperliquidTransaction:SpotSend": [
                    {"name": "hyperliquidChain", "type": "string"},
                    {"name": "destination", "type": "string"},
                    {"name": "token", "type": "string"},
                    {"name": "amount", "type": "string"},
                    {"name": "time", "type": "uint64"},
                ],
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
            },
            "primaryType": "HyperliquidTransaction:SpotSend",
            "message": action,
        }
        
        sig = self.wallet.sign_message(encode_structured_data(data))
        signature = {
            "r": to_hex(sig["r"]),
            "s": to_hex(sig["s"]),
            "v": sig["v"]
        }
        
        return {
            "method": "post",
            "id": timestamp,
            "request": {
                "type": "action",
                "payload": {
                    "action": action,
                    "nonce": timestamp,
                    "signature": signature,
                    "vaultAddress": None
                }
            }
        }

    def prepare_transfers(self, transfer_list: List[Tuple[str, float]], token: str, decimals: int) -> List[Dict]:
        """Prepare all transfers for sending"""
        transfers = []
        start_time = int(time.time() * 1000)
        
        for i, (dest, amount) in enumerate(transfer_list):
            timestamp = start_time + i
            formatted_amount = f"{amount:.{decimals}f}"
            transfers.append(self.create_signed_transfer(dest, formatted_amount, token, timestamp))
            
        return transfers

    def validate_token(self, token_name: str) -> Tuple[str, int]:
        """Validate token and get its details"""
        response = requests.post(
            f"{self.api_base['rest']}/info",
            headers=Config.HEADERS,
            json={"type": "spotMeta"}
        )
        response.raise_for_status()
        
        tokens = {t["name"]: t for t in response.json()["tokens"]}
        if token_name not in tokens:
            raise ValueError(f"Token {token_name} not found")
            
        return f"{token_name}:{tokens[token_name]['tokenId']}", tokens[token_name]['weiDecimals']

    def check_balance(self, token: str, required_amount: float) -> bool:
        """Check if wallet has sufficient balance"""
        response = requests.post(
            f"{self.api_base['rest']}/info",
            headers=Config.HEADERS,
            json={"type": "spotClearinghouseState", "user": self.wallet.address}
        )
        response.raise_for_status()
        
        balances = {b["coin"]: float(b["total"]) for b in response.json()["balances"]}
        if balances.get(token, 0) < required_amount:
            print(f"Insufficient {token} balance (only has {balances.get(token, 0)} and need {required_amount})")
            return False
        return True

    def check_rate_limit(self, num_transfers: int) -> bool:
        """Check if within rate limits"""
        response = requests.post(
            f"{self.api_base['rest']}/info",
            headers=Config.HEADERS,
            json={"type": "userRateLimit", "user": self.wallet.address}
        )
        response.raise_for_status()
        
        data = response.json()
        capacity = data["nRequestsCap"] - data["nRequestsUsed"]
        if capacity < num_transfers :
            print(f"Insufficient rate limit capacity (onyly has {capacity} and need {num_transfers})")
            return False
        return True

def read_transfers(file_path: str) -> List[Tuple[str, float]]:
    """Read transfers from CSV file"""
    transfers = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                try:
                    address = row[0].strip()
                    amount = float(row[1].strip())
                    transfers.append((address, amount))
                except ValueError:
                    continue
    return transfers

async def main():
    parser = argparse.ArgumentParser(description='Batch token transfer processor')
    parser.add_argument('private_key', help='Private key for signing')
    parser.add_argument('csv_file', help='CSV file with transfers (address,amount)')
    parser.add_argument('--token', default='USDC', help='Token name (default: USDC)')
    parser.add_argument('--mainnet', action='store_true', help='Use mainnet')
    args = parser.parse_args()

    try:
        # Initialize
        private_key = args.private_key[2:] if args.private_key.startswith('0x') else args.private_key
        wallet = Account.from_key(private_key)
        manager = TransferManager(wallet, args.mainnet)
        logger = Logger()
        
        print(f"Using wallet: {wallet.address}")
        
        # Load and validate data
        transfers = read_transfers(args.csv_file)
        if not transfers:
            print("No valid transfers found in CSV")
            return
        
        # Validate token and check limits
        token_id, decimals = manager.validate_token(args.token)

        print(f"\nLoaded {len(transfers)} transfers")
        print("\nFirst 5 transfers:")
        for addr, amt in transfers[:5]:
            print(f"{addr}: {amt:.{decimals}f}")
            
        total_amount = sum(amt for _, amt in transfers)
        print(f"\nTotal amount: {total_amount:.{decimals}f}")
        
        if not manager.check_balance(args.token, total_amount):
            return
            
        if not manager.check_rate_limit(len(transfers)):
            return
            
        # Prepare transfers
        if input("\nProceed with signing? (yes/no): ").lower() != 'yes':
            return
            
        print("\nPreparing transfers...")
        signed_transfers = manager.prepare_transfers(transfers, token_id, decimals)
        
        print("\nSample transfer:")
        print(json.dumps(signed_transfers[0], indent=2))
        
        if input("\nProceed with sending? (yes/no): ").lower() != 'yes':
            return
            
        # Process transfers
        print(f"\nConnecting to {manager.api_base['ws']}")
        await manager.process_bulk_transfers(signed_transfers, logger)
        
        # Write summary
        logger.write_summary()
        print(f"\nDetailed logs: {logger.log_file}")
        print(f"View on chain: https://{'' if args.mainnet else 'testnet.'}hypurrscan.io/address/{wallet.address}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())