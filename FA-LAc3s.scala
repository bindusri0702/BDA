val kivaLoansRdd = sc.textFile("C:/Users/bindu/Desktop/SEM4/BDA_214/project/kiva_final.csv")
val kivaRdd = kivaLoansRdd.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
val kivaSchemaRdd = kivaRdd.map{ k =>
val st = k.split(',')
val (id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds) = (st(0).toInt,st(1).toInt,st(2).toInt,
st(3),st(4),st(5),st(6),st(7),st(8),st(9),st(10),st(11).toInt,st(12).toInt,st(13),st(14),st(15))
(id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds)}
kivaSchemaRdd.toDF.show()


val countryFaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds) =>
(cntr,fa)}.reduceByKey(_+_)
val countryLaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds) =>
(cntr,la)}.reduceByKey(_+_)
val countryHighestLa = countryLaRdd.sortBy(_._2,ascending = false)
countryHighestLa.take(4)
val countryHighestLaRdd = countryLaRdd.sortBy(_._2,ascending = false)
val top10highestLaCountries = countryHighestLa.take(10)
 val countryHighestFaRdd = countryFaRdd.sortBy(_._2,ascending = false)
 val  top10highestFaCountries = countryHighestFaRdd.take(10)


