val genderWiseFaRdd = kivaSchemaRdd.map{ case(id,fa,la,act,sec,use,cncd,cntr,curr,pt,dt,ft,tim,bg,rint,ds) =>
(bg,fa)}.reduceByKey(_+_)
val Loan = genderWiseFaRdd.take(2)
val FemaleLoan = Loan(0)._2
val MaleLoan = Loan(1)._2
val LoanRatioByGender = FemaleLoan/MaleLoan
