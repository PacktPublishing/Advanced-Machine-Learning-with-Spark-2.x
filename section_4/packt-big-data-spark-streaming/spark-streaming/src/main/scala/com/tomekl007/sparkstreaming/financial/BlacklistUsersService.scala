package com.tomekl007.sparkstreaming.financial

object BlacklistUsersService {
  val blacklistedUsers = List("666", "123", "234")


  def userIsNotBlacklisted(userId: String) = {
    !blacklistedUsers.contains(userId)
  }

}
