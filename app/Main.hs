{-# language OverloadedStrings #-}

module Main where

import Database.Rodb.Writer

import qualified Data.Vector.Unboxed as V

main :: IO ()
main = do
  let conf = Config
        { keySizeBytes = 1
        , prefixSizeBits = 2
        , valSizeBytes = 8
        }
  withRodb "delme.rodb" conf (V.fromList [3,1,2,4]) $ \db -> do
    insert db "\x03" "   world"
    insert db "\x01" "someword"
    insert db "\xF2" "goodbye!"
    insert db "\x02" "hello   "
    insert db "\xE2" "goodbye3"
    insert db "\xD2" "goodbye2"
    insert db "\xC2" "goodbye1"
