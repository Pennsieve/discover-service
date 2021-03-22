// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.downloads

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ ByteString, ByteStringBuilder }
import scala.concurrent.Future

import java.io.ByteArrayOutputStream
import java.util.zip.{ ZipEntry, ZipOutputStream }

/**
  * Streamed ZIP archive creation
  *
  * This is a wraps a Java ZipOutputStream.
  */
object ZipStream {

  /**
    * The path of the entry in the ZIP file, and a Source that produces the
    * binary data for that entry.
    */
  type ZipSource = (String, Source[ByteString, NotUsed])

  /**
    *  Basic operations that can be performed while creating a ZIP archive
    */
  sealed trait ZipOperations {}
  case class StartEntry(name: String) extends ZipOperations
  case class StopEntry(name: String) extends ZipOperations
  case class WriteBytes(bytes: ByteString) extends ZipOperations
  case object CloseZip extends ZipOperations

  def apply(): Flow[ZipSource, ByteString, NotUsed] =
    Flow[ZipSource]
      .via(buildZipOperations())
      .via(executeZipOperations)

  /**
    * For each file added to the ZIP archive, perform the following operations:
    * 1) Start a new entry in the archive
    * 2) Write the bytes of the file
    * 3) Close the entry
    * Then, after all files have been added, close the ZIP archive.
    */
  def buildZipOperations(): Flow[ZipSource, ZipOperations, NotUsed] =
    Flow[ZipSource]
      .flatMapConcat {
        case (name, byteSource) =>
          byteSource
            .map(WriteBytes)
            .prepend(Source.single(StartEntry(name)))
            .concat(Source.single(StopEntry(name)))
      }
      .concat(Source.single(CloseZip))

  def executeZipOperations: Flow[ZipOperations, ByteString, NotUsed] = {
    Flow[ZipOperations].statefulMapConcat(() => {
      val builder = new ByteStringBuilder()
      val zipStream = new ZipOutputStream(builder.asOutputStream)

      operation: ZipOperations => {
        operation match {
          case StartEntry(name) => zipStream.putNextEntry(new ZipEntry(name))
          case StopEntry(name) => zipStream.closeEntry()
          case WriteBytes(bytes) =>
            zipStream.write(bytes.toArray, 0, bytes.length)
          case CloseZip => zipStream.close()
        }

        // Flush the ZIP stream to the underlying buffer, read the contents of
        // the buffer, clear the buffer, and send the underlying bytes onwards.
        // It is OK to use "fromArrayUnsafe" because the underlying byte array is
        // not mutated after the ByteString is created.
        zipStream.flush()
        val output = List(builder.result())
        builder.clear()
        output
      }
    })
  }
}
