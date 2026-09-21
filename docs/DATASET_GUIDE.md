# Dataset Guide

## customers.csv
Approximately 3,000 logical customers plus a few newer duplicate versions. Contains a small number of missing/dirty fields.

## products.json
Approximately 600 logical products, including a nested `category` object. Contains one deliberately invalid price and one newer duplicate version.

## orders.csv
Approximately 50,000 logical orders plus several newer duplicate versions. Contains a small number of deliberately invalid or orphan records.

The exact issue locations are intentionally not documented in the student package. Profiling and validation should discover them.
