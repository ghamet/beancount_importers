import beangulp
import dateutil
import locale

from beangulp.importers import csv
from beancount.core import data
from beancount.ingest.importers.csv import Importer as IngestImporter, Col as IngestCol
from beancount.core.amount import Amount
from beancount.core.number import Decimal
from beancount.core.position import Cost
from beancount_importers.bank_classifier import payee_to_account_mapping

# from beancount_paypal import PaypalImporter, lang

# locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
# locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')
# locale.setlocale(locale.LC_MONETARY, 'currency_symbol', 'EUR')

# CONFIG = [
#     PaypalImporter(
#         email_address='georg.hametner@googlemail.com',
#         account='Assets:US:PayPal',
#         checking_account='Assets:EU:Comdirect:Checking',
#         commission_account='Expenses:Financial:Commission',
#         language=lang.de(),
#         metadata_map={
#             "uuid": "Transaktionscode",
#             "sender": "Absender E-Mail-Adresse",
#             "recipient": "Empf??nger E-Mail-Adresse"
#         }
#     )
# ]
Col = csv.Col

# PYTHONPATH=.:beangulp python3 import_paypal.py <csv_file> > wise.bean

TRANSACTIONS_CLASSIFIED_BY_ID = {
    "CARD-XXXXXXXXX": "Expenses:Shopping",
}

# # UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:Uncategorized:Wise"
UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:FIXME"

def categorizer(txn, row):
    transaction_id = row[0]
    payee = row[13]
    comment = row[4]
    note = row[17]
    if not payee and comment.startswith("Sent money to "):
        payee = comment[14:]

    posting_account = None
    amount = Amount(Decimal(float(txn.postings[0].units.number)/100.00), 'EUR')
    # amount = locale.currency(txn.postings[0].units.number, symbol=True, grouping=False)

    if txn.postings[0].units.number < 0:
        # Expenses
        posting_account = payee_to_account_mapping.get(payee)

        # Custom
        # if payee == "Some Gym That Sells Food":
        #     if txn.postings[0].units.number < -40:
        #         posting_account = "Expenses:Wellness"
        #     else:
        #         posting_account = "Expenses:EatingOut"

        # Classify transfers
        # if payee.lower() == "your name":
        #     if "Revolut" in comment:
        #         posting_account = "Assets:Revolut:Cash"
        #     else:
        #         posting_account = "Assets:Wise:Cash"
        # elif payee == "Broker":
        #     posting_account = "Assets:Broker:Cash"
        # elif payee.lower() == "some dude":
        #     posting_account = "Liabilities:Shared:SomeDude"

        # if comment.endswith("to my savings jar"):
        #     posting_account = "Assets:Wise:Savings:USD"

        # Specific transactions
        if transaction_id in TRANSACTIONS_CLASSIFIED_BY_ID:
            posting_account = TRANSACTIONS_CLASSIFIED_BY_ID[transaction_id]

        # Default by category
        if not posting_account:
            posting_account = UNCATEGORIZED_EXPENSES_ACCOUNT
    else:
        if transaction_id in TRANSACTIONS_CLASSIFIED_BY_ID:
            posting_account = TRANSACTIONS_CLASSIFIED_BY_ID[transaction_id]
        elif comment.endswith("USD jar"):
            posting_account = "Assets:EU:Comdirect:Checking"
        else:
            posting_account = 'Income:Uncategorized:Wise'
            pass

    txn.postings.append(
        data.Posting(posting_account, amount, None, None, None, None)
    )
    if note:
        txn.meta['comment'] = note

    return txn

IMPORTER = csv.CSVImporter(
    {
        Col.DATE: "Datum",
        Col.NARRATION: "Artikelbezeichnung",
        Col.AMOUNT: "Brutto",
        Col.PAYEE: "Name",
        Col.CURRENCY: "W??hrung",
        Col.REFERENCE_ID: "Transaktionscode",
        Col.BALANCE: "Guthaben",
    },
    "Assets:EU:Comdirect:Checking",
    "EUR",
    categorizer=categorizer,
    dateutil_kwds={"parserinfo": dateutil.parser.parserinfo(dayfirst=True)},
)

def get_ingest_importer(account, currency):
    return IngestImporter(
        {
            IngestCol.DATE: "Datum",
            IngestCol.NARRATION: "Artikelbezeichnung",
            IngestCol.AMOUNT: "Brutto",
            IngestCol.PAYEE: "Name",
            IngestCol.REFERENCE_ID: "Transaktionscode",
            IngestCol.BALANCE: "Guthaben",
        },
        account,
        currency,
        categorizer=categorizer,
        dateutil_kwds={"parserinfo": dateutil.parser.parserinfo(dayfirst=True)},
    )

if __name__ == "__main__":
    ingest = beangulp.Ingest([IMPORTER], [])
    ingest()
