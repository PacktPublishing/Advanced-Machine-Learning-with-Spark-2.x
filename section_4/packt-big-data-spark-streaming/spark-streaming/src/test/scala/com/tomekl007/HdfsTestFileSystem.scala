package com.tomekl007

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}

trait HdfsTestFileSystem {
  implicit val fs: LocalFileSystem = FileSystem.getLocal(new Configuration())
  private val tmpdir = System.getProperty("java.io.tmpdir")

  def cleanupFs(): Unit = {
    fs.delete(testPath(), true)
  }

  def testPath(): Path = {
    new Path(s"$tmpdir/${getClass.getSimpleName}/")
  }

  def testPath(part: String): String = {
    s"$tmpdir/${getClass.getSimpleName}/$part/"
  }
}
