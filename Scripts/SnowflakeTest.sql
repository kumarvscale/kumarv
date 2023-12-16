--scale_playpen.goldfinch_holdings.trade_data 

    -- FILEPATH: /Users/vishalkumar/Documents/Git-Repo/kumarv/Scripts/SnowflakeTest.sql
    -- BEGIN: abpxx6d04wxr
    SELECT security_instrument_identifier, AVG(trade_price_amount) as avg_trade_price
    FROM scale_playpen.goldfinch_holdings.trade_data 
    WHERE security_instrument_identifier = '013817AW1' AND b_as_of_date = (SELECT max(b_as_of_date) FROM scale_playpen.goldfinch_holdings.trade_data ) AND trade_date >= DATEADD('week', -52, CURRENT_DATE())
    GROUP BY 1
    -- END: abpxx6d04wxr
