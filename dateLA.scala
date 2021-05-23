val dateLaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(ds,la)}
val highestLa = dateLaRdd.sortBy(_._2,ascending = false)
highestLa.take(5)
