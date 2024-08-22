#!/usr/bin/env bash

symbol=$1

payload(){
    token=$1
    https https://data.alpaca.markets/v2/stocks/bars \
        APCA-API-KEY-ID:@.id  \
        APCA-API-SECRET-KEY:@.key \
        accept:application/json \
        symbols=="${symbol}" \
        timeframe==1Day \
        start=='2010-01-01' \
        limit==1000 \
        adjustment==raw \
        feed==sip \
        sort==asc \
        page_token=="$token"
}

mkdir -p "data/${symbol}" 2>/dev/null

payload_var=$(payload)
next_token=$(echo $payload_var | jq -r .next_page_token)

while [[ "$next_token" != null ]]; 
do
    data_file_name="data/${symbol}/${next_token}_${symbol}.parquet"

    payload_var=$(payload $next_token)
    next_token=$(echo $payload_var | jq -r .next_page_token)

    echo $payload_var | jq 'del(.next_page_token)' | jq .bars."${symbol}" | jq -r '(map(keys)|add|unique)as$k|$k,(.[]|[.[$k[]]])|@csv' | \
    duckdb :memory "COPY (SELECT * FROM read_csv('/dev/stdin')) TO '/dev/stdout' WITH (FORMAT PARQUET, COMPRESSION 'SNAPPY')" \
    > ${data_file_name}

    duckdb /tmp/blah.db """
        CREATE TABLE IF NOT EXISTS ticker(
            close double precision,
            high double precision,
            low double precision,
            number integer,
            open double precision,
            ts timestamp,
            volume integer,
            volume_weighted double precision,
            symbol varchar
        );
        CREATE TEMP TABLE to_insert AS
        SELECT 
            c,
            h,
            l,
            n,
            o,
            t,
            v,
            vw, 
            '${symbol}' as symbol
        FROM read_parquet('${data_file_name}');
        SELECT
            *
        FROM to_insert 
        LIMIT 5;
        INSERT INTO ticker
        SELECT
            *
        FROM to_insert
        WHERE t || symbol NOT IN (
            SELECT ts || symbol FROM ticker GROUP BY 1
        );
    """
    echo "Done page ${next_token}"
done