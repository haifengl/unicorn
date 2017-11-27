name := "unicorn-rocksdb"

// 5.x series crash on newIterator
libraryDependencies += "org.rocksdb" % "rocksdbjni" % "4.13.5"

