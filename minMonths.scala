
val minMonthsRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds) =>
	(cntr,tim)}.reduceByKey(_+_).sortBy(_._2)

minMonthsRdd.toDF("Country","Term in Months").show()
