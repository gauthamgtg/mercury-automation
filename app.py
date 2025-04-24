import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mercury_api.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('MercuryAPI')

class TransactionKind(str, Enum):
    EXTERNAL_TRANSFER = "externalTransfer"
    INTERNAL_TRANSFER = "internalTransfer"
    OUTGOING_PAYMENT = "outgoingPayment"
    CREDIT_CARD_CREDIT = "creditCardCredit"
    CREDIT_CARD_TRANSACTION = "creditCardTransaction"
    DEBIT_CARD_TRANSACTION = "debitCardTransaction"
    INCOMING_DOMESTIC_WIRE = "incomingDomesticWire"
    CHECK_DEPOSIT = "checkDeposit"
    INCOMING_INTERNATIONAL_WIRE = "incomingInternationalWire"
    TREASURY_TRANSFER = "treasuryTransfer"
    WIRE_FEE = "wireFee"
    CARD_INTERNATIONAL_TRANSACTION_FEE = "cardInternationalTransactionFee"
    OTHER = "other"

class TransactionStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    CANCELLED = "cancelled"
    FAILED = "failed"

class AttachmentType(str, Enum):
    CHECK_IMAGE = "checkImage"
    RECEIPT = "receipt"
    OTHER = "other"

@dataclass
class Address:
    address1: str
    address2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    region: Optional[str] = None
    postalCode: Optional[str] = None
    country: Optional[str] = None

@dataclass
class DomesticWireRoutingInfo:
    bankName: Optional[str] = None
    accountNumber: Optional[str] = None
    routingNumber: Optional[str] = None
    address: Optional[Address] = None

@dataclass
class ElectronicRoutingInfo:
    accountNumber: Optional[str] = None
    routingNumber: Optional[str] = None
    bankName: Optional[str] = None

@dataclass
class CorrespondentInfo:
    routingNumber: Optional[str] = None
    swiftCode: Optional[str] = None
    bankName: Optional[str] = None

@dataclass
class BankDetails:
    bankName: Optional[str] = None
    cityState: Optional[str] = None
    country: Optional[str] = None

@dataclass
class CountrySpecificData:
    countrySpecificDataCanada: Optional[Dict[str, str]] = None
    countrySpecificDataAustralia: Optional[Dict[str, str]] = None
    countrySpecificDataIndia: Optional[Dict[str, str]] = None
    countrySpecificDataRussia: Optional[Dict[str, str]] = None
    countrySpecificDataPhilippines: Optional[Dict[str, str]] = None
    countrySpecificDataSouthAfrica: Optional[Dict[str, str]] = None

@dataclass
class InternationalWireRoutingInfo:
    iban: Optional[str] = None
    swiftCode: Optional[str] = None
    correspondentInfo: Optional[CorrespondentInfo] = None
    bankDetails: Optional[BankDetails] = None
    address: Optional[Address] = None
    phoneNumber: Optional[str] = None
    countrySpecific: Optional[CountrySpecificData] = None

@dataclass
class CardInfo:
    id: Optional[str] = None

@dataclass
class TransactionDetails:
    address: Optional[Address] = None
    domesticWireRoutingInfo: Optional[DomesticWireRoutingInfo] = None
    electronicRoutingInfo: Optional[ElectronicRoutingInfo] = None
    internationalWireRoutingInfo: Optional[InternationalWireRoutingInfo] = None
    debitCardInfo: Optional[CardInfo] = None
    creditCardInfo: Optional[CardInfo] = None

@dataclass
class CurrencyExchangeInfo:
    convertedFromCurrency: str
    convertedToCurrency: str
    convertedFromAmount: float
    convertedToAmount: float
    feeAmount: float
    feePercentage: float
    exchangeRate: float
    feeTransactionId: str

@dataclass
class Attachment:
    fileName: str
    url: str
    attachmentType: AttachmentType

@dataclass
class Transaction:
    amount: float
    bankDescription: Optional[str] = None
    counterpartyId: Optional[str] = None
    counterpartyName: Optional[str] = None
    counterpartyNickname: Optional[str] = None
    createdAt: datetime = None
    dashboardLink: Optional[str] = None
    details: Optional[TransactionDetails] = None
    estimatedDeliveryDate: Optional[datetime] = None
    failedAt: Optional[datetime] = None
    id: Optional[str] = None
    kind: Optional[TransactionKind] = None
    note: Optional[str] = None
    externalMemo: Optional[str] = None
    postedAt: Optional[datetime] = None
    reasonForFailure: Optional[str] = None
    status: Optional[TransactionStatus] = None
    feeId: Optional[str] = None
    currencyExchangeInfo: Optional[CurrencyExchangeInfo] = None
    compliantWithReceiptPolicy: Optional[bool] = None
    hasGeneratedReceipt: Optional[bool] = None
    creditAccountPeriodId: Optional[str] = None
    mercuryCategory: Optional[str] = None
    generalLedgerCodeName: Optional[str] = None
    attachments: Optional[List[Attachment]] = None
    relatedTransactions: Optional[List[str]] = None  # List of related transaction IDs

@dataclass
class TransactionResponse:
    total: int
    transactions: List[Transaction]

class MercuryAPIClient:
    """Client for interacting with the Mercury API to fetch bank transactions."""
    
    BASE_URL = "https://api.mercury.com/api/v1"
    
    def __init__(self, api_key: str):
        """Initialize the Mercury API client with the provided API key.
        
        Args:
            api_key: Your Mercury API key
        """
        logger.info("Initializing Mercury API client")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_accounts(self) -> List[Dict[str, Any]]:
        """Fetch all available bank accounts.
        
        Returns:
            List of account objects
        """
        logger.info("Fetching all available bank accounts")
        url = f"{self.BASE_URL}/accounts"
        response = self.make_request(url)
        accounts = response.get("accounts", [])
        logger.info(f"Successfully retrieved {len(accounts)} accounts")
        return accounts
    
    def get_transactions(self, 
                         account_id: Optional[str] = None, 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         limit: int = 500,
                         offset: int = 0,
                         order: str = "desc") -> TransactionResponse:
        """Fetch transactions with optional filtering.
        
        Args:
            account_id: Required account ID to fetch transactions for
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            limit: Number of transactions to retrieve (max 500)
            offset: Number of transactions to skip
            order: Sort order ('asc' or 'desc')
            
        Returns:
            TransactionResponse object containing transaction data
        """
        logger.info(f"Fetching transactions - Limit {limit}, Offset {offset}, Order {order}")
        logger.debug(f"Filters - Account ID: {account_id}, Start Date: {start_date}, End Date: {end_date}")
        
        if not account_id:
            logger.error("Account ID is required for fetching transactions")
            raise ValueError("Account ID is required for fetching transactions")
            
        url = f"{self.BASE_URL}/account/{account_id}/transactions"
        params = {
            "limit": limit,
            "offset": offset,
            "order": order
        }
        
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        response = self.make_request(url, params=params)
        logger.info(f"Successfully retrieved transactions")
        
        # Convert response to TransactionResponse object
        transactions = []
        for t in response.get("transactions", []):
            # Convert string dates to datetime objects
            for date_field in ["createdAt", "estimatedDeliveryDate", "failedAt", "postedAt"]:
                if t.get(date_field):
                    t[date_field] = datetime.fromisoformat(t[date_field].replace("Z", "+00:00"))
            
            # Convert kind and status to enums
            if t.get("kind"):
                t["kind"] = TransactionKind(t["kind"])
            if t.get("status"):
                t["status"] = TransactionStatus(t["status"])
            
            # Convert attachments
            if t.get("attachments"):
                t["attachments"] = [
                    Attachment(
                        fileName=a["fileName"],
                        url=a["url"],
                        attachmentType=AttachmentType(a["attachmentType"])
                    )
                    for a in t["attachments"]
                ]
            
            transactions.append(Transaction(**t))
        
        return TransactionResponse(
            total=response.get("total", 0),
            transactions=transactions
        )
    
    def get_all_transactions(self, 
                            account_id: Optional[str] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> List[Transaction]:
        """Fetch all transactions across all pages.
        
        Args:
            account_id: Required account ID to fetch transactions for
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            
        Returns:
            List of Transaction objects
        """
        logger.info("Starting to fetch all transactions")
        
        if not account_id:
            logger.error("Account ID is required for fetching transactions")
            raise ValueError("Account ID is required for fetching transactions")
            
        all_transactions = []
        offset = 0
        limit = 500  # Maximum allowed by API
        has_more = True
        
        while has_more:
            logger.debug(f"Fetching transactions with offset {offset}")
            response = self.get_transactions(
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset
            )
            
            all_transactions.extend(response.transactions)
            
            # If we got fewer transactions than the limit, we've reached the end
            has_more = len(response.transactions) == limit
            offset += limit
            
        logger.info(f"Successfully retrieved all {len(all_transactions)} transactions")
        return all_transactions
    
    def make_request(self, url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a request to the Mercury API.
        
        Args:
            url: API endpoint URL
            params: Optional query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            Exception: If the API request fails
        """
        logger.debug(f"Making API request to {url} with params: {params}")
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            logger.debug("API request successful")
            return response.json()
        else:
            error_message = f"API request failed with status code {response.status_code}: {response.text}"
            logger.error(error_message)
            raise Exception(error_message)
    
    def save_transactions_to_csv(self, transactions: List[Transaction], filename: str = "transactions.csv"):
        """Save transactions to a CSV file.
        
        Args:
            transactions: List of Transaction objects
            filename: Name of the CSV file to save
        """
        logger.info(f"Attempting to save {len(transactions)} transactions to {filename}")
        
        if not transactions:
            logger.warning("No transactions to save")
            print("No transactions to save.")
            return
        
        try:
            # Convert transactions to dictionaries
            transaction_dicts = []
            for t in transactions:
                t_dict = t.__dict__.copy()
                # Convert datetime objects to strings
                for date_field in ["createdAt", "estimatedDeliveryDate", "failedAt", "postedAt"]:
                    if t_dict.get(date_field):
                        t_dict[date_field] = t_dict[date_field].isoformat()
                # Convert enums to strings
                if t_dict.get("kind"):
                    t_dict["kind"] = t_dict["kind"].value
                if t_dict.get("status"):
                    t_dict["status"] = t_dict["status"].value
                transaction_dicts.append(t_dict)
            
            df = pd.DataFrame(transaction_dicts)
            df.to_csv(filename, index=False)
            logger.info(f"Successfully saved {len(transactions)} transactions to {filename}")
            print(f"Saved {len(transactions)} transactions to {filename}")
        except Exception as e:
            logger.error(f"Failed to save transactions to CSV: {str(e)}")
            raise
        
    def display_transaction_summary(self, transactions: List[Transaction]):
        """Display a summary of the transactions.
        
        Args:
            transactions: List of Transaction objects
        """
        logger.info("Generating transaction summary")
        
        if not transactions:
            logger.warning("No transactions found for summary")
            print("No transactions found.")
            return
        
        print(f"Found {len(transactions)} transactions")
        
        # Calculate total amount
        total_amount = sum(t.amount for t in transactions)
        
        # Group by status
        status_counts = {}
        for t in transactions:
            status = t.status.value if t.status else "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info(f"Transaction summary - Total amount: ${total_amount:.2f}, Status counts: {status_counts}")
        print(f"Total amount: ${total_amount:.2f}")
        print("Transaction statuses:")
        for status, count in status_counts.items():
            print(f"  - {status}: {count}")


def main():
    """Main function to run the Mercury API transaction fetcher."""
    logger.info("Starting Mercury API transaction fetcher")
    
    # Get API key from environment variable or input
    api_key = os.environ.get("MERCURY_API_KEY")
    if not api_key:
        logger.info("API key not found in environment variables, requesting user input")
        api_key = input("Enter your Mercury API key: ")
    
    # Create client
    client = MercuryAPIClient(api_key)
    
    try:
        # Fetch accounts
        accounts = client.get_accounts()
        print(f"Found {len(accounts)} accounts")
        
        # Print account information
        for i, account in enumerate(accounts):
            print(f"{i+1}. {account.get('name')} (ID: {account.get('id')})")
        
        # Ask for account selection
        account_choice = input("\nEnter account number to fetch transactions for (or press Enter for all accounts): ")
        selected_account_id = None
        if account_choice and account_choice.isdigit() and 1 <= int(account_choice) <= len(accounts):
            selected_account_id = accounts[int(account_choice) - 1].get('id')
            logger.info(f"Selected account ID: {selected_account_id}")
            print(f"Selected account: {accounts[int(account_choice) - 1].get('name')}")
        
        # Ask for date range
        default_start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        start_date = input(f"\nEnter start date (YYYY-MM-DD) or press Enter for default ({default_start_date}): ")
        if not start_date:
            start_date = default_start_date
            
        end_date = input("Enter end date (YYYY-MM-DD) or press Enter for today: ")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"Fetching transactions for date range: {start_date} to {end_date}")
        print(f"\nFetching transactions from {start_date} to {end_date}...")
        
        # Fetch all transactions
        transactions = client.get_all_transactions(
            account_id=selected_account_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Display transaction summary
        client.display_transaction_summary(transactions)
        
        # Save transactions to CSV
        save_option = input("\nDo you want to save transactions to CSV? (y/n): ")
        if save_option.lower() == 'y':
            filename = input("Enter filename (default: transactions.csv): ")
            if not filename:
                filename = "transactions.csv"
            client.save_transactions_to_csv(transactions, filename)
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        print(f"Error: {e}")


if __name__ == "__main__":
    main()