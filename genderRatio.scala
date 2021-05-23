val genderRatio = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(bg,fa)}.reduceByKey(_+_)

genderRatio.toDF.show()