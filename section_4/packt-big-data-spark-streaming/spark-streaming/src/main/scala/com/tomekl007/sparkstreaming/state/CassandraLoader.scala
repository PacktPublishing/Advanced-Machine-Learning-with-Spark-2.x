package com.tomekl007.sparkstreaming.state

import com.datastax.driver.core.{Cluster, Session}

class CassandraLoader(val session: Session) {

  def selectAllFromTimeSeries(idToSelect: Int): List[UserEvent] = {
    val cqlStatement: String = s"SELECT * FROM userevent where id = $idToSelect"

    val row = session
      .execute(cqlStatement).one()
    val id: Int = row.getInt("id")
    val data: String = row.getString("data")
    val isLast: Boolean = row.getBool("isLast")
    List(UserEvent(id, data, isLast))

  }


  def insertNewEvent(event: UserSession): Unit = {
    event.userEvents.foreach(e => {
      session.execute(s"insert into userevent (id, data, isLast) " +
        s"values (${
          e.id
        }, '${
          e.data
        }', ${
          e.isLast
        })")
    })
  }

}


object CassandraLoader {
  def connect(): CassandraLoader = {
    val serverIP: String = "127.0.0.1"
    val keyspace: String = "test"

    val cluster: Cluster = Cluster.builder.addContactPoints(serverIP).build

    new CassandraLoader(cluster.connect(keyspace))
  }
}