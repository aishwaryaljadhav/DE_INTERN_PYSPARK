from array_dataset import df
from pyspark.sql.functions import*

df2=df.select('*', array('Skill1','Skill2','Skill3').alias('Skills')).show()

df2.select('*',
    array_contains('Skills', 'Python').alias('Has Python'),
    array_position('Skills', 'SQL').alias('SQL_position'),
    array_remove('Skills', 'SQL').alias('without SQL'),
    size('Skills').alias('Total_skills')
          ).show(truncate=False)

