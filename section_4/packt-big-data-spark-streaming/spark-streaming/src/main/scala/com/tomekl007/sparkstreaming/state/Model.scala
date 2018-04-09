package com.tomekl007.sparkstreaming.state

case class UserSession(userEvents: Seq[UserEvent])

case class UserEvent(id: Int, data: String, isLast: Boolean)

