# Triple CLI Transaction Enricher

This is a Command Line Interface (CLI) tool designed to process financial transactions by batch using the Triple Enrichment API.

## Features

- **Batch Processing**: Reads transactions from a CSV file.
- **Concurrent Processing**: Uses multi-threading to enrich multiple transactions in parallel for faster processing.
- **Enrichment**: Enriches transaction data with clean merchant names, categories, and logos using the Triple API.
- **Error Handling**: Handles rate limiting (429) with automatic retries and validates input data.
- **Output**: Generates a CSV file with the original data plus enrichment results.

## Prerequisites

- Python 3.6 or higher
- `pip` (Python package installer)

## Installation

1. Clone the repository or download the files.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the script using the following command format:

```bash
python triple_enricher.py --input <INPUT_CSV> --output <OUTPUT_CSV> --token <API_TOKEN> --url <API_URL> --workers <NUM_WORKERS>
```

### Arguments

- `-i`, `--input`: Path to the input CSV file containing transactions.
- `-o`, `--output`: Path where the enriched CSV output will be saved.
- `-t`, `--token`: Your Triple API Token.
- `-u`, `--url`: The Triple API Endpoint URL (e.g., `https://api.sandbox.tripledev.app/api/v1/enrich-transaction/`).
- `-w`, `--workers`: Number of concurrent workers/threads (default: 5).

### Example

```bash
python triple_enricher.py \
  --input input.csv \
  --output enriched_transactions.csv \
  --token tr_test_fdsklsdaa8c3bc53fb73930828c92ecce6fcde612 \
  --url https://api.sandbox.tripledev.app/api/v1/enrich-transaction/
```

## Input CSV Format

The input CSV should contain at least the following columns:

- `merchant_name`: The raw merchant name from the transaction.
- `transaction_type`: One of `BANK_TRANSFER`, `CARD_TRANSACTION`, `INVOICE`.
- `transaction_id`: A unique identifier for the transaction (if missing, the tool will generate a UUID for each row).

Optional columns (sent to API if present):
- `transaction_amount`
- `transaction_currency`
- `merchant_country`
- `merchant_city`
- ... (see Triple API docs for full list)

## Output

The output CSV will contain all original columns plus data flattened from the Triple API response.

### Included Fields (Enrichment Results)

*   `transaction_id`: Included (or generated if missing) for tracking.
*   `enrichment_status`: `success`, `error`, or `skipped`.
*   `enrichment_error`: Error details if the request failed (e.g., `Validation failed: Invalid merchant_country length: 2 (expected 3)`, `400 Bad Request`, etc.).

**Visual Enrichments:**
*   `clean_name`: The cleaned merchant name.
*   `category`: The merchant category.
*   `logo_url`: Link to the merchant's logo.
*   `brand_id`: The Brand ID.
*   `default_logo`: Boolean indicating if it's a default logo.

**Merchant Location:**
*   `location_enabled`
*   `location_id`
*   `location_country`, `location_city`, `location_street`, `location_zip`
*   `location_lat`, `location_lon`

**Other Details:**
*   `subscription_enabled`, `subscription_recurring`
*   `co2_enabled`, `co2_emissions`
*   `fraud_enabled`, `fraud_flagged`
*   `all_categories`
*   `contact_enabled`, `contact_email`, `contact_phone`, `contact_website`
*   `processor_enabled`, `processor_name`, `processor_logo`, `processor_brand_id`

## Time Spent

| Activity | Time |
| :--- | :--- |
| Technical Documentation Study | 1.5 hours |
| Tool Development | 4 hours |
| **Total** | **5.5 hours** |

