from join_dataset import df_customers, df_orders

print('INNER JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "inner").show()

print('LEFT JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "left").select(df_customers.Name, df_orders.Product).show()

print('RIGHT JOIN')

df_customers.join(df_orders,
             'Customer_ID',
             "right").show()

