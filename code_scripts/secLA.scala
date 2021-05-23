val sectorLaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(sec,la)}.reduceByKey(_+_).sortBy(_._2,ascending = false)

sectorLaRdd.toDF("sector name","Loan amount").show()

