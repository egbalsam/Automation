USE STOCK
DECLARE @begdate as date,
		@enddate as date,
		@minval as decimal,
		@maxval as decimal,
		@tick as varchar(32)

SET		@tick = '^GSPC'
SET 	@begdate = (SELECT MIN(dtdate) FROM STOCKS WHERE strtick = @tick)
SET		@enddate = (SELECT MAX(dtdate) FROM STOCKS WHERE strtick = @tick)
SET		@minval = (SELECT CASE WHEN LEFT(MIN(decAdjClose),1)=1 THEN POWER(10,LEN(MIN(decAdjClose))-1) ELSE POWER(10,LEN(MIN(decAdjClose))) END FROM STOCKS WHERE strtick = @tick)
SET		@maxval = (SELECT CASE WHEN LEFT(MAX(decAdjClose),1)=9 THEN POWER(10,LEN(MAX(decAdjClose))) ELSE POWER(10,LEN(MAX(decAdjClose))-1) END FROM STOCKS WHERE strtick = @tick)



SELECT
	strtick
	,LEFT(decAdjClose,1) AS BUCKET
	,COUNT(strtick) AS BENFORD_COUNT
FROM
	STOCKS
WHERE
	strTick = @tick
	AND dtDate BETWEEN @begdate AND @enddate
	AND decAdjClose BETWEEN @minval AND @maxval
GROUP BY
	strTick
	,LEFT(decAdjClose,1)
ORDER BY
	1,2