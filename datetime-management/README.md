# how to use Flink SQL to handle dates

Example using Flink SQL  to transform datetimes expressed in String to timestamp, considering timezones or not

```SQL
-- defining our local time zone to guarantee the right value
SET 'sql.local-time-zone' = 'UTC';

select
       -- original string
       '2024-12-25T10:30:12.1239876Z' as control
       -- as is transformation, just remove the extra string
       , TO_TIMESTAMP(SUBSTRING(REPLACE('2024-12-25T10:30:12.1239876Z', 'T', ' '), 1, 25)) as as_is_conversion
       -- remove the extra precision, and convert 
       , FROM_UNIXTIME(CAST(UNIX_TIMESTAMP(LEFT('2024-12-25T10:30:12.1239876Z',23)||'Z', 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS BIGINT),  'yyyy-MM-dd HH:mm:ss.SSS') as conversion_without_tz
       -- simplified date already with miliseconds only
       ,FROM_UNIXTIME(UNIX_TIMESTAMP('2024-12-25T10:30:12.123Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX'), 'yyyy-MM-dd HH:mm:ss.SSS') as conversion_Z_value_for_tz
       ,FROM_UNIXTIME(UNIX_TIMESTAMP('2024-12-25T10:30:12.123+01', 'yyyy-MM-dd''T''HH:mm:ss.SSSX'), 'yyyy-MM-dd HH:mm:ss.SSS') as conversion_2_values_for_tz
       ,FROM_UNIXTIME(UNIX_TIMESTAMP('2024-12-25T10:30:12.123+0130', 'yyyy-MM-dd''T''HH:mm:ss.SSSZ'), 'yyyy-MM-dd HH:mm:ss.SSS') as conversion_4_values_for_tz
```

## RESULTS
```bash 
control                    : 2024-12-25T10:30:12.1239876Z
original_conversion        : 2024-12-25 10:30:12.123
fixed_conversion           : 2024-12-25 10:30:12.000
conversion_Z_value_for_tz  : 2024-12-25 10:30:12.000
conversion_2_values_for_tz : 2024-12-25 09:30:12.000
conversion_4_values_for_tz : 2024-12-25 09:00:12.000  
```


> [!NOTE]
> * Review  https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html to see how to check the pattern 
> * UNIX_TIMESTAMP has seconds precision
