val mnthLaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(ds.split('-')(1).toInt,la)}.reduceByKey(_+_)

mnthLaRdd.take(5)