package com.tomekl007.sparkstreaming.abandonedCart

import com.tomekl007.{WithId, WithUserId}

sealed trait CartEvent extends WithUserId

case class AddToCart(userId: String) extends CartEvent

case class RemoveFromCart(userId: String) extends CartEvent

case class AbandonedCartNotification(id: String) extends WithId