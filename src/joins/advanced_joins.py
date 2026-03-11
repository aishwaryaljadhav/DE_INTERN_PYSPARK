from join_dataset import df_customers, df_orders

print('CROSS JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "cross").show()

print('FULL OUTER JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "outer").show()

print('LEFT-ANTI JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "left_anti").show()

print('LEFT SEMI JOIN')

df_customers.join(df_orders,
             df_customers.Customer_ID==df_orders.Customer_ID,
             "left_semi").show()

