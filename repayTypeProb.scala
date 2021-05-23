val repayTypeRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(rint)}
val irregularRepayCount = repayTypeRdd.filter{x => x == "irregular"}.count()
val monthlyRepayCount = repayTypeRdd.filter{x => x == "monthly"}.count()
val weeklyRepayCount = repayTypeRdd.filter{x => x == "weekly"}.count()
val bulletRepayCount = repayTypeRdd.filter{x => x == "bullet"}.count()
val irregularRatio = irregularRepayCount.toFloat/repayTypeRdd.count().toFloat*100
val monthlyRatio = (monthlyRepayCount.toFloat/repayTypeRdd.count().toFloat)*100
val bulletRatio = (bulletRepayCount.toFloat/repayTypeRdd.count().toFloat)*100

