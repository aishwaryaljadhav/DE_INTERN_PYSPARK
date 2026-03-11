from math_dataset import df
from pyspark.sql.functions import*

df.select('*',
        abs('Discount').alias('Abs_Discount'),
        ceil('Weight').alias('Rounded_weight'),
        floor('Weight').alias('Floored_weight'),
        sqrt('Price').alias('sqrt_price'),
        log('Price').alias('log_price'),
        power('Price',2).alias('increased_price'),
        exp('Weight').alias('Exp_weight')
          ).show()