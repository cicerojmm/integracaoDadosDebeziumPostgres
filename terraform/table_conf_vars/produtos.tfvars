conf_tables = {
  conf_name = "EMR-DELTALAKE"
  conf_schema = [
   {
     topic_name = "vendas.public.item_vendas",
     primary_keys = [
       "id"
     ]
   },
   {
     topic_name = "vendas.public.produtos",
     primary_keys = [
       "id"
     ],
     big_table = "True"
   },  
]
}

  
