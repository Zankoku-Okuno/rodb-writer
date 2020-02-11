{-# language BangPatterns #-}
{-# language OverloadedStrings #-}

module Main where

import Options.Applicative
import Database.Rodb.Writer

import Control.Monad (when)
import System.IO (hPutStrLn,hPutStr,stderr)

import qualified Data.ByteString.Lazy as LBS

main :: IO ()
main = do
  (conf, ifile, ofile) <- execParser options
  putErrLn "scanning for bucket sizes"
  bucketSizes <- (\bs -> scanRawRows bs conf) <$> LBS.readFile ifile
  putErrLn "create header and prefix table"
  withRodb ofile conf bucketSizes $ \db -> do
    let rowcnt = numRows (sizes db)
    let rowSize = fromIntegral $ keySizeBytes conf + valSizeBytes conf
        loop !i inp
          | LBS.length inp < rowSize = putErrLn "DONE ☺"
          | otherwise = do
              when (i `mod` (max 1 $ rowcnt `div` 100) == 0) $
                putErr "."
              insert db (LBS.toStrict $ LBS.take (fromIntegral $ keySizeBytes conf) inp) (LBS.toStrict $ LBS.take (fromIntegral $ valSizeBytes conf) $ LBS.drop (fromIntegral $ keySizeBytes conf) inp)
              loop (i + 1) $ LBS.drop rowSize inp
    loop 0 =<< LBS.readFile ifile


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

putErrLn :: String -> IO ()
putErrLn = hPutStrLn stderr

putErr :: String -> IO ()
putErr = hPutStr stderr