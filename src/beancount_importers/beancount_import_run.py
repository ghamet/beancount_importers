#!/usr/bin/env python3

import os
from pathlib import Path

import yaml
import click

import beancount_import.webserver

from uabean.importers import (
    ibkr,
    binance,
    monobank,
    kraken,
)

import beancount_importers.import_monzo as import_monzo
import beancount_importers.import_wise as import_wise
import beancount_importers.import_revolut as import_revolut
import beancount_importers.import_comdirect as import_comdirect
import beancount_importers.import_sparda as import_sparda
import beancount_importers.import_paypal as import_paypal
import beancount_importers.import_google as import_google
import beancount_importers.import_amazon as import_amazon

def get_importer_config(type, account, currency, importer_params):
    if type == 'monzo':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_monzo.get_ingest_importer(account, currency, importer_params),
            description=(
                "In the app go to Help > Download a statement. "
                "The easiest way would be just to download monthly statements every month."
            ),
            emoji='💷'
        )
    elif type == 'wise':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_wise.get_ingest_importer(account, currency),
            description='Can be downloaded online from https://wise.com/balances/statements',
            emoji='💵'
        )
    elif type == 'revolut':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_revolut.get_ingest_importer(account, currency),
            emoji='💵'
        )
    elif type == 'ibkr':
        return dict(
            module='beancount_import.source.generic_importer_source_beangulp',
            importer=ibkr.Importer(use_existing_holdings=False, **(importer_params or {})),
            description=(
                'Go to Performance & Reports > Flex Queries. '
                'Create new one. Enable "Interest accruals", "Cash Transactions", "Trades", "Transfers". '
                'From "Cash Transactions" disable fields "FIGI", "Issuer Country Code", "Available For Trading Date". '
                'From "Trades" disable "Sub Category", "FIGI", "Issuer Country Code", "Related Trade ID", '
                '"Orig *", "Related Transaction ID", "RTN", "Initial Investment". Otherwise importer may break.'
            ),
            emoji='📈'
        )
    elif type == 'monobank':
        mapped_account_config = {}
        for p in importer_params.get('account_config', []):
            tp = p[0]
            currency = p[1]
            account = p[2]
            mapped_account_config[(tp, currency)] = account
        mapped_params = importer_params.copy()
        mapped_params['account_config'] = mapped_account_config
        return dict(
            module='beancount_import.source.generic_importer_source_beangulp',
            importer=monobank.Importer(**mapped_params),
            emoji='💵'
        )
    elif type == 'kraken':
        return dict(
            module='beancount_import.source.generic_importer_source_beangulp',
            importer=kraken.Importer(**(importer_params or {})),
            emoji='🎰'
        )
    elif type == 'binance':
        return dict(
            module='beancount_import.source.generic_importer_source_beangulp',
            importer=binance.Importer(**(importer_params or {})),
            emoji='🎰'
        )
    elif type == 'sparda':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_sparda.get_ingest_importer(account, currency),
            emoji='💵'
        )
    elif type == 'comdirect':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_comdirect.MultiImporter(import_comdirect.CHECKING, account),
            emoji='💵'
        )
    elif type == 'paypal':
        mapped_account_config = {}
        for p in importer_params.get('account_config', []):
            tp = p[0]
            email_address = p[1]
            account = p[2]
            checking_account = p[3]
            commission_account = p[4]
            currency = p[5]
            mapped_account_config[(tp, currency)] = account
        mapped_params = importer_params.copy()
#        mapped_params['account_config'] = mapped_account_config
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_amazon.get_ingest_importer(account, currency),
#            importer=import_paypal.PaypalImporter(**mapped_params),
            emoji='💵'
        )
    elif type == 'amazon':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_amazon.get_ingest_importer(account, currency),
            emoji='💵'
        )
    elif type == 'google':
        return dict(
            module='beancount_import.source.generic_importer_source',
            importer=import_google.get_ingest_importer(account, currency),
            emoji='💵'
        )
    else:
        return None

def load_import_config_from_file(filename, data_dir, output_dir):
    with open(filename, 'r') as config_file:
        parsed_config = yaml.safe_load(config_file)
        data_sources = []
        for key, params in parsed_config['importers'].items():
            config = dict(
                account=params['account'],
                directory=os.path.join(data_dir, key),
                **get_importer_config(params['importer'], params.get('account'), params.get('currency'), params.get('params'))
            )
            data_sources.append(
                config
            )
        return dict(
            all=dict(
                data_sources=data_sources,
                transactions_output=os.path.join(output_dir, 'transactions.bean')
            )
        )

def get_import_config(data_dir, output_dir):
    import_config = {
        'paypal': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.paypal',
                    importer=import_paypal.PaypalImporter(data_dir, 'Assets:EU:Comdirect:Checking', 'EUR'),
                    directory=os.path.join(data_dir, 'paypal'),
                    account='Assets:Paypal',
                    fee_account='Expenses:Financial:Paypal:Fees',
                    prefix='paypal',
                    locale='de_DE' # optional, default: 'en_US'
                )
            ],
            transactions_output=os.path.join(output_dir, 'paypal', 'transactions.bean')
        ),
        'amazon': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.amazon',
                    importer=import_amazon.get_ingest_importer('Assets:EU:Comdirect:Checking', 'EUR'),
                    amazon_account='georg.hametner@gmail.com',
                    account='Assets:Amazon',
                    prefix='amazon',
                    directory=os.path.join(data_dir, 'amazon'),
                    posttax_adjustment_accounts=dict(
                        gift_card='Assets:Gift-Cards:Amazon',
                        rewards_points='Income:Amazon:Cashback',
                    ),
                ),
            ],
            transactions_output=os.path.join(output_dir, 'amazon', 'transactions.bean')
        ),
        'google': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.google',
                    importer=import_google.get_ingest_importer('Assets:EU:Comdirect:Checking', 'EUR'),
                    directory=os.path.join(data_dir, 'google'),
                    account='Assets:Google',
                    fee_account='Expenses:Shopping:Google:Fees',
                    prefix='google',
                    locale='de_DE' # optional, default: 'en_US'
                )
            ],
            transactions_output=os.path.join(output_dir, 'google', 'transactions.bean')
        ),
        'sparda': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.sparda',
                    importer=import_sparda.get_ingest_importer('Assets:EU:Sparda:Checking', 'EUR'),
                    directory=os.path.join(data_dir, 'sparda'),
                    account='Assets:EU:Sparda:Checking',
                    prefix='sparda',
                    locale='de_DE'
                )
            ],
            transactions_output=os.path.join(output_dir, 'sparda', 'transactions.bean')
        ),
        'comdirect': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.comdirect',
                    importer=import_comdirect,
                    directory=os.path.join(data_dir, 'comdirect'),
                    account='Assets:EU:Comdirect:Checking',
                    fee_account='Expenses:Financial:Fees',
                    prefix='comdirect',
                    locale='de_DE'
                )
            ],
            transactions_output=os.path.join(output_dir, 'comdirect', 'transactions.bean')
        ),
        'monzo': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_monzo.get_ingest_importer('Assets:Monzo:Cash', 'GBP'),
                    account='Assets:Monzo:Cash',
                    directory=os.path.join(data_dir, 'monzo')
                )
            ],
            transactions_output=os.path.join(output_dir, 'monzo', 'transactions.bean')
        ),
        'wise_usd': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_wise.get_ingest_importer('Assets:Wise:Cash', 'USD'),
                    account='Assets:Wise:Cash',
                    directory=os.path.join(data_dir, 'wise_usd')
                )
            ],
            transactions_output=os.path.join(output_dir, 'wise_usd', 'transactions.bean')
        ),
        'wise_gbp': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_wise.get_ingest_importer('Assets:Wise:Cash', 'GBP'),
                    account='Assets:Wise:Cash',
                    directory=os.path.join(data_dir, 'wise_gbp')
                )
            ],
            transactions_output=os.path.join(output_dir, 'wise_gbp', 'transactions.bean')
        ),
        'wise_eur': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_wise.get_ingest_importer('Assets:Wise:Cash', 'EUR'),
                    account='Assets:Wise:Cash',
                    directory=os.path.join(data_dir, 'wise_eur')
                )
            ],
            transactions_output=os.path.join(output_dir, 'wise_eur', 'transactions.bean')
        ),
        'revolut_usd': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_revolut.get_ingest_importer('Assets:Revolut:Cash', 'USD'),
                    account='Assets:Revolut:Cash',
                    directory=os.path.join(data_dir, 'revolut_usd')
                )
            ],
            transactions_output=os.path.join(output_dir, 'revolut', 'transactions.bean')
        ),
        'revolut_gbp': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_revolut.get_ingest_importer('Assets:Revolut:Cash', 'GBP'),
                    account='Assets:Revolut:Cash',
                    directory=os.path.join(data_dir, 'revolut_gbp')
                )
            ],
            transactions_output=os.path.join(output_dir, 'revolut', 'transactions.bean')
        ),
        'revolut_eur': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source',
                    importer=import_revolut.get_ingest_importer('Assets:Revolut:Cash', 'EUR'),
                    account='Assets:Revolut:Cash',
                    directory=os.path.join(data_dir, 'revolut_eur')
                )
            ],
            transactions_output=os.path.join(output_dir, 'revolut', 'transactions.bean')
        ),
        'ibkr': dict(
            data_sources=[
                dict(
                    module='beancount_import.source.generic_importer_source_beangulp',
                    importer=ibkr.Importer(),
                    account='Assets:IB',
                    directory=os.path.join(data_dir, 'ibkr')
                )
            ],
            transactions_output=os.path.join(output_dir, 'ibkr', 'transactions.bean')
    ),
    }
    import_config_all = dict(
        data_sources=[],
        transactions_output=os.path.join(output_dir, 'transactions.bean')
    )
    for k, v in import_config.items():
        import_config_all['data_sources'].extend(v['data_sources'])

    import_config['all'] = import_config_all
    return import_config
    
@click.command()
@click.option(
    "--journal_file", 
    type=click.Path(), 
    default='main.bean',
    help="Path to your main ledger file"
)
@click.option(
    "--importers_config_file", 
    type=click.Path(), 
    default=None,
    help="Path to the importers config file"
)
@click.option(
    "--data_dir", 
    type=click.Path(), 
    default='beancount_import_data', 
    help="Directory with your import data (e.g. bank statements in csv)"
)
@click.option(
    "--output_dir", 
    type=click.Path(), 
    default='beancount_import_output',
    help="Where to put output files (don't forget to include them in your main ledger)"
)
@click.option(
    "--target_config", 
    default="all", 
    help="Note that specifying particular config will also result in transactions " + 
    "being imported into specific output file for that config"
)
@click.option(
    "--address", 
    default="127.0.0.1", 
    help="Web server address"
)
@click.option(
    "--port", 
    default="8101", 
    help="Web server port"
)
def main(port, address, target_config, output_dir, data_dir, importers_config_file, journal_file):
    import_config = None
    if importers_config_file:
        import_config = load_import_config_from_file(importers_config_file, data_dir, output_dir)
    else:
        import_config = get_import_config(data_dir, output_dir)
    # Create output structure if it doesn't exist
    os.makedirs(os.path.dirname(import_config[target_config]['transactions_output']), exist_ok=True)
    Path(import_config[target_config]['transactions_output']).touch()
    for file in ['accounts.bean', 'balance_accounts.bean', 'prices.bean', 'ignored.bean']:
        Path(os.path.join(output_dir, file)).touch()

    beancount_import.webserver.main(
        {},
        port=port,
        address=address,
        journal_input=journal_file,
        ignored_journal=os.path.join(output_dir, 'ignored.bean'),
        default_output=import_config[target_config]['transactions_output'],
        open_account_output_map=[
            ('.*', os.path.join(output_dir, 'accounts.bean')),
        ],
        balance_account_output_map=[
            ('.*', os.path.join(output_dir, 'balance_accounts.bean')),
        ],
        price_output=os.path.join(output_dir, 'prices.bean'),
        data_sources=import_config[target_config]['data_sources'],
    )
    
if __name__ == '__main__':
    main()
