package com.tomekl007.sparkstreaming.financial

import com.tomekl007.{WithId, WithUserId}

case class Payment(userId: String, to: String, amount: BigDecimal) extends WithUserId

case class PaymentValidated(id: String,
                            to: String,
                            amount: BigDecimal
                           )
  extends WithId

object PaymentValidated {
  def fromPayment: (Payment) => PaymentValidated = {
    p => PaymentValidated(p.userId, p.to, p.amount)
  }

}
