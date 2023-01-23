CREATE FUNCTION FtoC(@Celcius dec(6,3))
RETURNS dec(6,3)
AS
BEGIN
DECLARE
@Fahrenheit dec(6,3)
SET @Fahrenheit=(@Celcius*9/5)+32
RETURN @Fahrenheit
END

--DROP FUNCTION FtoC

DECLARE @CELC as DEC(6,3)
SET @CELC = 0

SELECT @CELC AS Celcius, [dbo].ftoc(@CELC) as Fahrenheit