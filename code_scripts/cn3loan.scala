/*val kivaLoansRdd = sc.textFile("C:/Users/pooja/OneDrive/Desktop/SEM4/BigDataAna_214/proj/kiva1-60k.csv")
val kivaRdd = kivaLoansRdd.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
val kivaSchemaRdd = kivaRdd.map{ k =>
val st = k.split(',')
val (id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds) = (st(0).toInt,st(1).toInt,st(2).toInt,
st(3),st(4),st(5),st(6),st(7),st(8),st(9),st(10),st(11),st(12).toInt,st(13).toInt,st(14),st(15),st(16))
(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,lenCnt,bg,rint,ds)}*/

val countLoansRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,cncd,cntr,curr,pt,dt,ft,lenCnt,tim,bg,rint,ds) =>
	(cntr,la)}.groupByKey()

val c1 = countLoansRdd.take(2)

val countryWiseLoanCount = countLoansRdd.map{ c =>
	val cntry = c._1
	val loans = c._2
	val count = loans.size
	(cntry,count)}

countryWiseLoanCount.toDF("Country","Number of times Loan taken").show()