#!/usr/bin/env python3

# Copyright (C) 2021 Daniel Kraft <d@domob.eu>
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.

"""
Command-line utility for working with ArDrive CLI data folders and
toggling the "cloud only" flag.
"""

import argparse
import contextlib
import os.path
import sqlite3
import sys

# Name of the ArDrive SQLite database file.
DB_FILE = ".ardrive-cli.db"


def parseArgs ():
  """
  Configures the argument parser and runs it to return the parsed
  arguments.
  """

  desc = "Handles 'cloud only' flag for ArDrive CLI folders"

  parser = argparse.ArgumentParser (description=desc)
  parser.add_argument ("action", choices=["ls", "cloud", "local"],
                       help="The action to perform")
  parser.add_argument ("path", nargs="?", default=".",
                       help="The path to operate on")

  return parser.parse_args ()


def getDbFile (path):
  """
  Tries to locate the ArDrive CLI database file in the given directory,
  looking through parent folders until it is found.  Returns None if
  it cannot be found at all.
  """

  while True:
    trial = os.path.join (path, DB_FILE)
    if os.path.exists (trial):
      return trial

    newPath = os.path.dirname (path)
    if newPath == path:
      return None

    path = newPath


def rowToDict (row):
  """
  Converts a database row instance to a dict.
  """

  return {
    row.keys ()[n]: row[n]
    for n in range (len (row))
  }


def getLatestForId (db, fileId):
  """
  Returns the latest (current) row in the sync table for the given
  fileId.  The row is returned as dict.
  """

  with contextlib.closing (db.cursor ()) as cur:
    cur.execute ("""
      SELECT `id`, `entityType`,
             `fileId`, `fileName`, `filePath`, `parentFolderId`
        FROM `sync`
        WHERE `fileId` = ?
        ORDER BY `unixTime` DESC
        LIMIT 1
    """, (fileId,))
    row = cur.fetchone ()
    if row is None:
      return None
    return rowToDict (row)


def getIdForPath (db, path):
  """
  Returns the file ID corresponding to the ArDrive folder
  that matches the given local path.
  """

  with contextlib.closing (db.cursor ()) as cur:
    cur.execute ("""
      SELECT `fileId`
        FROM `sync`
        WHERE `filePath` = ?
    """, (path,))
    for row in cur:
      fileId = row[0]
      data = getLatestForId (db, fileId)
      if data["filePath"] == path:
        return fileId

  return None


def getFolderContent (db, folderId):
  """
  Returns all current contents of a folder with the given ID,
  with the data fields as per getLatestForId.
  """

  res = []

  with contextlib.closing (db.cursor ()) as cur:
    cur.execute ("""
      SELECT DISTINCT `fileId`
        FROM `sync`
        WHERE `parentFolderId` = ?
    """, (folderId,))

    for row in cur:
      data = getLatestForId (db, row[0])
      if data["parentFolderId"] != folderId:
        continue

      res.append (data)

  return res


def performLs (db, baseId):
  """
  Performs an "ls" operation, which looks for all contents
  in the base folder.
  """

  files = []
  folders = []

  content = getFolderContent (db, baseId)
  for c in content:
    if c["entityType"] == "file":
      files.append (c["fileName"])
    elif c["entityType"] == "folder":
      folders.append (c["fileName"])

  files.sort ()
  folders.sort ()

  for nm in folders:
    print (nm + "/")
  for nm in files:
    print (nm)


def markContentsAs (db, baseId, value):
  """
  Marks a folder (and all its contents) as cloudOnly or
  not cloudOnly (based on value).
  """

  idsToMark = []
  idsToMark.append (getLatestForId (db, baseId)["id"])

  content = getFolderContent (db, baseId)
  for c in content:
    if c["entityType"] == "file":
      idsToMark.append (c["id"])
    elif c["entityType"] == "folder":
      markContentsAs (db, c["fileId"], value)

  db.executemany ("""
    UPDATE sync
      SET cloudOnly = ?
      WHERE id = ?
  """, [(value, x) for x in idsToMark])


def performCloud (db, baseId):
  """
  Marks a folder (and all its contents) as cloudOnly.
  """

  markContentsAs (db, baseId, 1)


def performLocal (db, baseId):
  """
  Marks a folder and all its contents, as well as its direct
  ancestors up to the drive root as not cloudOnly.
  """

  markContentsAs (db, baseId, 0)

  idsToMark = []
  parentId = getLatestForId (db, baseId)["parentFolderId"]
  while True:
    data = getLatestForId (db, parentId)
    if data is None:
      break
    idsToMark.append (data["id"])
    parentId = data["parentFolderId"]

  db.executemany ("""
    UPDATE sync
      SET cloudOnly = 0
      WHERE id = ?
  """, [(x, ) for x in idsToMark])


if __name__ == "__main__":
  args = parseArgs ()
  path = os.path.abspath (args.path)

  dbFile = getDbFile (path)
  if dbFile is None:
    sys.exit (f"Could not locate an ArDrive database file in {path}")

  with contextlib.closing (sqlite3.connect (dbFile)) as db:
    db.row_factory = sqlite3.Row
    baseId = getIdForPath (db, path)
    if args.action == "ls":
      performLs (db, baseId)
    elif args.action == "cloud":
      performCloud (db, baseId)
    elif args.action == "local":
      performLocal (db, baseId)
    if args.action != "ls":
      db.commit ()
