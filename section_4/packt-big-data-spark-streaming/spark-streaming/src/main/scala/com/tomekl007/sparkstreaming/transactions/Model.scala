package com.tomekl007.sparkstreaming.transactions

import com.tomekl007.WithId

case class Transaction(userId: String, itemId: String, amount: BigDecimal)

case class TopSeller(id: String, description: String, discount: BigDecimal) extends WithId

case class TopSellingAggregate(count: Int, sumOfAmounts: BigDecimal)
