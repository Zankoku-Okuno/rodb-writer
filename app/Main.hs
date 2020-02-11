{-# language OverloadedStrings #-}

module Main where

import Options.Applicative
import Database.Rodb.Writer

import qualified Data.ByteString.Lazy as LBS

main :: IO ()
main = do
  (conf, ifile, ofile) <- execParser options
  bucketSizes <- (\bs -> scanRawRows bs conf) <$> LBS.readFile ifile
  withRodb ofile conf bucketSizes $ \db -> do
    let rowSize = fromIntegral $ keySizeBytes conf + valSizeBytes conf
        loop inp
          | LBS.length inp < rowSize = pure ()
          | otherwise = do
              insert db (LBS.toStrict $ LBS.take (fromIntegral $ keySizeBytes conf) inp) (LBS.toStrict $ LBS.take (fromIntegral $ valSizeBytes conf) $ LBS.drop (fromIntegral $ keySizeBytes conf) inp)
              loop $ LBS.drop rowSize inp
    loop =<< LBS.readFile ifile
    -- insert db "\x03" "   world"
    -- insert db "\x01" "someword"
    -- insert db "\xF2" "goodbye!"
    -- insert db "\x02" "hello   "
    -- insert db "\xE2" "goodbye3"
    -- insert db "\xD2" "goodbye2"
    -- insert db "\xC2" "goodbye1"


options :: ParserInfo (Config, FilePath, FilePath)
options = info (argParser <**> helper)
   ( fullDesc
  <> progDesc "Convert an unordered file of key-value pairs to an RODB-format \"database\"."
  <> header "rodb-writer - create RODB files"
  )

argParser :: Parser (Config, FilePath, FilePath)
argParser = (,,) <$>
  configParser
  <*> strOption
    ( long "input-file"
   <> short 'i'
   <> metavar "FILE"
   <> help "Input data file" )
  <*> strOption
    ( long "output-file"
   <> short 'o'
   <> metavar "FILE"
   <> help "Output data file" )

configParser :: Parser Config
configParser = Config
  <$> option auto
    ( long "key-size"
   <> short 'K'
   <> metavar "BYTECOUNT"
   <> help "Total number of bytes in the key" )
  <*> option auto
    ( long "prefix-size"
   <> short 'b'
   <> metavar "BITCOUNT"
   <> help "Number of leading bits to use for bucketing" )
  <*> option auto
    ( long "value-size"
   <> short 'V'
   <> metavar "BYTECOUNT"
   <> help "Number of bytes with which to store values" )
