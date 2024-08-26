#!/usr/bin/env bash

set_and_name=$(
    https api.scryfall.com/sets | \
    jq -r '.data| .[] | {"code": .code, "name": .name}' | \
    jq -s | \
    jq -r '(map(keys)|add|unique)as$k|$k,(.[]|[.[$k[]]])|@csv' | \
    fzf | \
    sed 's/"//g'
)

IFS=, read -r set name <<< "${set_and_name}"

insert_query="""
CREATE TABLE IF NOT EXISTS card_prices(
        card_name varchar,
        price double precision,
        card_rarity varchar,
        card_set varchar
    );
    CREATE TEMP TABLE to_insert AS
    SELECT
        name as card_name,
        price_usd,
        rarity as card_rarity,
        '${name}' as card_set
    FROM read_csv('/dev/stdin');
    SELECT
        *
    FROM to_insert
    LIMIT 5;
    INSERT INTO card_prices
    SELECT
        *
    FROM to_insert
    WHERE card_name||card_set NOT IN (
        SELECT card_name||card_set FROM card_prices GROUP BY 1
    );
"""

page_call=$(
    https api.scryfall.com/cards/search \
        order==usd \
        q=="set:${set}"
    )

data=$(
    echo "${page_call}" | jq '.data|.[]|{"name": .name,"rarity": .rarity, "price_usd": .prices.usd}' | jq -s | \
    jq -r '(map(keys)|add|unique)as$k|$k,(.[]|[.[$k[]]])|@csv'
)
next_page=$(echo "${page_call}" | jq -r .next_page)
echo "${data}" | duckdb /tmp/mtg.db "${insert_query}"

while [[ "${next_page}" != null ]];
do
    page_call=$(curl -s "${next_page}")
    data=$(
        echo "${page_call}" | jq '.data|.[]|{"name": .name,"rarity": .rarity, "price_usd": .prices.usd}' | jq -s | \
        jq -r '(map(keys)|add|unique)as$k|$k,(.[]|[.[$k[]]])|@csv'
    )
    echo "${data}" | duckdb /tmp/mtg.db "${insert_query}"
    next_page=$(echo "${page_call}" | jq -r .next_page)
done