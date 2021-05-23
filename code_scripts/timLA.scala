val loanTermMonthsRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(la,tim)}

loanTermMonthsRdd.toDF("loan amount","term in months").show()