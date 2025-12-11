#!/usr/bin/env python3
import argparse
import pandas as pd
import requests
import time
import uuid
import sys
import re
import datetime
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed


def validate_row(row) -> (bool, str):
    """
    Validates that the row contains the minimum required fields and the optional fields are valid.
    Returns: (is_valid, error_message)
    """

    # Required fields
    required_fields = ['merchant_name', 'transaction_type', 'transaction_id']
    for field in required_fields:
        if field not in row or pd.isna(row[field]):
            return False, f"Missing required field: {field}"
    
    # transaction_type enum
    valid_types = ['BANK_TRANSFER', 'CARD_TRANSACTION', 'INVOICE']
    if row['transaction_type'] not in valid_types:
        return False, f"Invalid transaction_type: {row['transaction_type']}"

    # merchant_country (ISO 3166-1 alpha-3)
    if 'merchant_country' in row and not pd.isna(row['merchant_country']):
        if not isinstance(row['merchant_country'], str):
            return False, "merchant_country must be a string"
        if len(row['merchant_country']) != 3:
            return False, f"Invalid merchant_country length: {len(row['merchant_country'])} (expected 3)"

    # merchant_category_code (optional, but if present must be exactly 4 digits)
    if 'merchant_category_code' in row and not pd.isna(row['merchant_category_code']):
        raw = str(row['merchant_category_code']).strip()

        if raw != "":
            # Must be exactly 4 digits
            if not re.fullmatch(r"\d{4}", raw):
                return False, (
                    f"Invalid merchant_category_code: {row['merchant_category_code']} "
                    "(must be exactly 4 digits)"
                )

    # merchant_city
    if 'merchant_city' in row and not pd.isna(row['merchant_city']):
        if not isinstance(row['merchant_city'], str):
            return False, "merchant_city must be a string"
        if len(row['merchant_city']) > 255:
            return False, "merchant_city exceeds 255 characters"

    # merchant_id
    if 'merchant_id' in row and not pd.isna(row['merchant_id']):
        if not isinstance(row['merchant_id'], str):
            return False, "merchant_id must be a string"
        if len(row['merchant_id']) > 255:
            return False, "merchant_id exceeds 255 characters"

    # transaction_timestamp (ISO8601 UTC)
    if 'transaction_timestamp' in row and not pd.isna(row['transaction_timestamp']):
        if not isinstance(row['transaction_timestamp'], str):
            return False, "transaction_timestamp must be a string"
        val = row['transaction_timestamp']
        try:
            datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                return False, "Invalid transaction_timestamp format (expected ISO8601 UTC)"

    # transaction_amount
    if 'transaction_amount' in row and not pd.isna(row['transaction_amount']):
        raw = str(row['transaction_amount']).strip()
        if not re.fullmatch(r"^-?\d{0,10}(?:\.\d{0,2})?$", raw):
            return False, "transaction_amount does not match expected decimal pattern"

    # transaction_currency (ISO 4217)
    if 'transaction_currency' in row and not pd.isna(row['transaction_currency']):
        if not isinstance(row['transaction_currency'], str):
            return False, "transaction_currency must be a string"
        if len(row['transaction_currency']) != 3:
            return False, f"Invalid transaction_currency length: {len(row['transaction_currency'])} (expected 3)"

    # transaction_reference_text (only valid for BANK_TRANSFER)
    if 'transaction_reference_text' in row and not pd.isna(row['transaction_reference_text']):
        if not isinstance(row['transaction_reference_text'], str):
            return False, "transaction_reference_text must be a string"
        if len(row['transaction_reference_text']) > 255:
            return False, "transaction_reference_text exceeds 255 characters"
        if row['transaction_type'] != 'BANK_TRANSFER':
            return False, "transaction_reference_text is only valid for transaction_type == 'BANK_TRANSFER'"

    # account_id
    if 'account_id' in row and not pd.isna(row['account_id']):
        if not isinstance(row['account_id'], str):
            return False, "account_id must be a string"
        if len(row['account_id']) > 255:
            return False, "account_id exceeds 255 characters"

    # channel_type
    if 'channel_type' in row and not pd.isna(row['channel_type']):
        if not isinstance(row['channel_type'], str):
            return False, "channel_type must be a string"
        if row['channel_type'] not in ['ATM', 'POS', 'ECOMMERCE']:
            return False, f"Invalid channel_type: {row['channel_type']}"

    # vat
    if 'vat' in row and not pd.isna(row['vat']):
        if not isinstance(row['vat'], str):
            return False, "vat must be a string"
        if len(row['vat']) > 30:
            return False, "vat exceeds 30 characters"

    return True, ""


def flatten_response(response_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens the nested API response to extract relevant fields.
    """
    flat_data = {}
    
    if 'transaction_id' in response_json:
        flat_data['transaction_id'] = response_json['transaction_id']

    # 1. Visual Enrichments
    if 'visual_enrichments' in response_json:
        ve = response_json['visual_enrichments']
        flat_data['clean_name'] = ve.get('merchant_clean_name')
        flat_data['category'] = ve.get('merchant_category')
        flat_data['logo_url'] = ve.get('merchant_logo_link')
        flat_data['brand_id'] = ve.get('brand_id')
        flat_data['default_logo'] = ve.get('default_logo')

    # 2. Merchant Location
    if 'merchant_location' in response_json:
        ml = response_json['merchant_location']
        flat_data['location_enabled'] = ml.get('enabled')
        flat_data['location_id'] = ml.get('location_id')
        
        if ml.get('address'):
            addr = ml['address']
            flat_data['location_country'] = addr.get('country')
            flat_data['location_city'] = addr.get('city')
            flat_data['location_street'] = addr.get('street')
            flat_data['location_zip'] = addr.get('zip_code')
            
        if ml.get('coordinates'):
            coords = ml['coordinates']
            flat_data['location_lat'] = coords.get('lat')
            flat_data['location_lon'] = coords.get('lon')

    # 3. Subscriptions
    if 'subscriptions' in response_json:
        subs = response_json['subscriptions']
        flat_data['subscription_enabled'] = subs.get('enabled')
        flat_data['subscription_recurring'] = subs.get('is_recurring')

    # 4. CO2 Footprint
    if 'co2_footprint' in response_json:
        co2 = response_json['co2_footprint']
        flat_data['co2_enabled'] = co2.get('enabled')
        flat_data['co2_emissions'] = co2.get('emissions')

    # 5. Fraud
    if 'fraud' in response_json:
        fraud = response_json['fraud']
        flat_data['fraud_enabled'] = fraud.get('enabled')
        flat_data['fraud_flagged'] = fraud.get('merchant_flagged')

    # 6. Categories
    if 'categories' in response_json:
        cats = response_json['categories']
        if isinstance(cats, list) and len(cats) > 0:
            flat_data['all_categories'] = ";".join([c.get('name', '') for c in cats if c.get('name')])

    # 7. Contact
    if 'contact' in response_json:
        contact = response_json['contact']
        flat_data['contact_enabled'] = contact.get('enabled')
        flat_data['contact_email'] = contact.get('email')
        flat_data['contact_phone'] = contact.get('phone')
        flat_data['contact_website'] = contact.get('website')

    # 8. Payment Processor
    if 'payment_processor' in response_json:
        pp = response_json['payment_processor']
        flat_data['processor_enabled'] = pp.get('enabled')
        flat_data['processor_name'] = pp.get('name')
        flat_data['processor_logo'] = pp.get('logo_url')
        flat_data['processor_brand_id'] = pp.get('brand_id')
    
    return flat_data


def enrich_transaction(row: Dict[str, Any], url: str, token: str) -> Dict[str, Any]:
    """
    Sends a request to the Triple API to enrich the transaction.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token {token}'
    }
    
    payload = {
        'merchant_name': row['merchant_name'],
        'transaction_type': row['transaction_type'],
        'transaction_id': str(row['transaction_id'])
    }
    
    # Optional fields
    optional_fields = [
        'merchant_country', 'merchant_category_code', 'merchant_city', 
        'merchant_id', 'transaction_timestamp', 'transaction_amount', 
        'transaction_currency', 'transaction_reference_text', 'account_id',
        'channel_type', 'vat'
    ]
    
    for field in optional_fields:
        if field in row and not pd.isna(row[field]):
            if field == 'transaction_amount':
                payload[field] = str(row[field])
            elif field == 'merchant_category_code':
                raw = str(row[field]).strip()
                if raw != "":
                    payload[field] = int(raw)
            else:
                payload[field] = row[field]

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)

            if response.status_code == 429:
                print("Rate limit reached. Waiting 10 seconds...")
                time.sleep(10)
                continue

            if response.status_code == 200:
                return {
                    'status': 'success',
                    'data': response.json()
                }
            else:
                return {
                    'status': 'error',
                    'error_code': response.status_code,
                    'error_message': response.text
                }

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return {
                    'status': 'error',
                    'error_message': str(e)
                }
            time.sleep(2)

    return {
        'status': 'error',
        'error_message': 'Max retries reached'
    }


def process_transaction(index: int, row_dict: Dict[str, Any], url: str, token: str):
    """
    Validates the row
    Calls the API
    Returns the result ready to be written to the DataFrame
    """
    is_valid, error_reason = validate_row(row_dict)
    if not is_valid:
        return index, 'skipped', f"Validation failed: {error_reason}", {}

    print(f"Enriching transaction {row_dict.get('transaction_id', index)}...")
    result = enrich_transaction(row_dict, url, token)

    if result['status'] == 'success':
        flat_data = flatten_response(result['data'])
        return index, 'success', None, flat_data
    else:
        error_msg = f"{result.get('error_code')} {result.get('error_message')}"
        return index, 'error', error_msg, {}


def main():
    parser = argparse.ArgumentParser(description='Triple API Transaction Enricher CLI')
    parser.add_argument('-i', '--input', required=True, help='Path to input CSV file')
    parser.add_argument('-o', '--output', required=True, help='Path to output CSV file')
    parser.add_argument('-t', '--token', required=True, help='API Token')
    parser.add_argument('-u', '--url', required=True, help='API Endpoint URL')
    parser.add_argument('-w', '--workers', type=int, default=5, help='Number of concurrent workers (threads)')
    
    args = parser.parse_args()
    
    try:
        df = pd.read_csv(
            args.input,
            dtype={"merchant_category_code": "string"}
        )
    except Exception as e:
        print(f"Error reading input CSV: {e}")
        sys.exit(1)

    # merchant_category_code: lo dejamos como string "tal cual", sin corregir nada.
    # Si pandas lo ha convertido a float, lo convertimos a string, y la validación decidirá.
    if 'merchant_category_code' in df.columns:
        df['merchant_category_code'] = df['merchant_category_code'].fillna("").astype(str).str.strip()

    # transaction_id
    if 'transaction_id' not in df.columns:
        print("Warning: 'transaction_id' column missing. Generating UUIDs for each row.")
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    else:
        # Fill missing transaction_ids in existing column
        missing_ids = df['transaction_id'].isna() | (df['transaction_id'].astype(str).str.strip() == '')
        if missing_ids.any():
            print(f"Warning: Found {missing_ids.sum()} rows with missing 'transaction_id'. Generating UUIDs for them.")
            df.loc[missing_ids, 'transaction_id'] = [str(uuid.uuid4()) for _ in range(missing_ids.sum())]

    # enrichment columns
    if 'enrichment_status' not in df.columns:
        df['enrichment_status'] = None
    if 'enrichment_error' not in df.columns:
        df['enrichment_error'] = None
        
    df['enrichment_status'] = df['enrichment_status'].astype('object')
    df['enrichment_error'] = df['enrichment_error'].astype('object')

    print(f"Processing {len(df)} transactions with {args.workers} workers...")

    futures = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for index, row in df.iterrows():
            if row.get('enrichment_status') == 'success':
                print(f"Skipping transaction {row.get('transaction_id', index)} (already enriched)")
                continue

            row_dict = row.to_dict()
            future = executor.submit(process_transaction, index, row_dict, args.url, args.token)
            futures[future] = index

        for future in as_completed(futures):
            index = futures[future]
            try:
                idx, status, error_msg, flat_data = future.result()
            except Exception as e:
                df.at[index, 'enrichment_status'] = 'error'
                df.at[index, 'enrichment_error'] = str(e)
                continue

            df.at[index, 'enrichment_status'] = status
            df.at[index, 'enrichment_error'] = error_msg

            for key, value in flat_data.items():
                df.at[index, key] = value

    # Save the updated DataFrame
    try:
        df.to_csv(args.output, index=False)
        print(f"Done! Results written to {args.output}")
    except Exception as e:
        print(f"Error writing output CSV: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()