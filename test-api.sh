#!/bin/bash

# Configuration
GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8093"} # Default to direct service port
API_KEY=${API_KEY:-""}
ZONE_ID=${ZONE_ID:-1}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}GridTokenX Trading Service - API Test${NC}"
echo -e "Target URL: ${GATEWAY_URL}"
echo "--------------------------------------------------------"

# Helper function to check responses
check_response() {
    local status=$1
    local expected=$2
    local label=$3
    if [ "$status" -eq "$expected" ]; then
        echo -e "[${GREEN}PASS${NC}] $label (Status: $status)"
    else
        echo -e "[${RED}FAIL${NC}] $label (Status: $status, Expected: $expected)"
        # Don't exit here to see other failures
    fi
}

# 1. Health Check
echo -e "\n${YELLOW}1. Testing Health Check...${NC}"
HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/health")
check_response "$HEALTH_RESPONSE" 200 "Health Check"

# 2. Market Stats
echo -e "\n${YELLOW}2. Testing Market Stats...${NC}"
curl -s -X GET \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/markets/stats" | jq .
STATS_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/markets/stats")
check_response "$STATS_RESPONSE" 200 "Market Stats"

# 3. Order Book
echo -e "\n${YELLOW}3. Testing Order Book for Zone ${ZONE_ID}...${NC}"
curl -s -X GET \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/markets/zones/${ZONE_ID}/order-book" | jq .
BOOK_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/markets/zones/${ZONE_ID}/order-book")
check_response "$BOOK_RESPONSE" 200 "Order Book"

# 4. Create Quote
echo -e "\n${YELLOW}4. Testing Create Quote...${NC}"
QUOTE_DATA='{
    "buyer_zone_id": 1,
    "seller_zone_id": 2,
    "energy_amount_kwh": "100.5",
    "agreed_price": "4.5"
}'
curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "X-API-KEY: ${API_KEY}" \
    -d "$QUOTE_DATA" \
    "${GATEWAY_URL}/api/v1/quotes" | jq .
QUOTE_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "X-API-KEY: ${API_KEY}" \
    -d "$QUOTE_DATA" \
    "${GATEWAY_URL}/api/v1/quotes")
check_response "$QUOTE_RESPONSE" 200 "Create Quote"

# 5. Submit Order
echo -e "\n${YELLOW}5. Testing Submit Order...${NC}"
ORDER_DATA='{
    "side": "buy",
    "order_type": "limit",
    "energy_amount_kwh": "50.0",
    "price_per_kwh": "4.2",
    "zone_id": '"$ZONE_ID"'
}'
SUBMIT_OUTPUT=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "X-API-KEY: ${API_KEY}" \
    -d "$ORDER_DATA" \
    "${GATEWAY_URL}/api/v1/orders")
echo "$SUBMIT_OUTPUT" | jq .
SUBMIT_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "X-API-KEY: ${API_KEY}" \
    -d "$ORDER_DATA" \
    "${GATEWAY_URL}/api/v1/orders")
check_response "$SUBMIT_RESPONSE" 200 "Submit Order"

# 6. List My Orders
echo -e "\n${YELLOW}6. Testing List My Orders...${NC}"
curl -s -X GET \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/users/me/orders?limit=5" | jq .
LIST_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-API-KEY: ${API_KEY}" \
    "${GATEWAY_URL}/api/v1/users/me/orders?limit=5")
check_response "$LIST_RESPONSE" 200 "List Orders"
