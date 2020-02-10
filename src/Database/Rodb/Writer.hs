{-# language BangPatterns #-}
{-# language DataKinds #-}
{-# language DuplicateRecordFields #-}
{-# language MagicHash #-}
{-# language NamedFieldPuns #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}

module Database.Rodb.Writer
  ( Rodb
  , Config(..)
  , withRodb
  , insert
  ) where

import Control.Monad (when)
import Data.Bits (countLeadingZeros, shiftR, shiftL)
import Data.Vector.Unboxed (Vector)
import Data.Word (Word8, Word32, Word64)
import Foreign.Ptr (Ptr, plusPtr, minusPtr, castPtr)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, moveBytes)
import Foreign.Storable (sizeOf, peekByteOff, pokeByteOff, pokeElemOff)
import Data.ByteString.Internal (ByteString(PS), memcmp)
import System.ByteOrder (Fixed(..), ByteOrder(..))
import System.IO.MMap (mmapFilePtr, Mode(ReadWrite), munmapFilePtr)
import System.Posix.Files (setFileSize)

import qualified Data.ByteString as BS
import qualified Data.Vector.Unboxed as V


data Rodb = Rodb
  { header :: Ptr (Fixed 'BigEndian Word32)
  , prefixTable :: Ptr Word8
  , dataTable :: Ptr Word8
  , rawSize :: Int
  , sizes :: Sizes
  }

data Config = Config
  { keySizeBytes :: Int
  , prefixSizeBits :: Int
  , valSizeBytes :: Int
  }

type BucketSizesElems = Vector Word64


data Sizes = Sizes
  -- configured
  { keySizeBytes :: Int
  , prefixSizeBits :: Int
  , valSizeBytes :: Int
  -- calculated
  , headerSizeBytes :: Int
  , offsetSizeBytes :: Int
  , prefixSizeBytes :: Int
  , prefixTableSizeBytes :: Int
  , suffixSizeBytes :: Int
  , alignSizeBytes :: Int
  , rowSizeBytes :: Int
  , dataTableSizeBytes :: Int
  , dbSizeBytes :: Int
  , prefixOffsetBytes :: Int
  , dataOffsetBytes :: Int
  , bucketSizes :: BucketSizesElems
  , numBuckets :: Int
  }


withRodb :: FilePath -> Config -> BucketSizesElems -> (Rodb -> IO a) -> IO a
withRodb filepath config bucketSizes k = do
  -- TODO check prefix size bits matches the log_2 of buckets length
  db@Rodb{header,rawSize} <- create
  v <- k db
  munmapFilePtr header rawSize
  pure v
  where
  create = do
    -- create and open the blank file
    let sizes = calculate config bucketSizes
    writeFile filepath ""
    setFileSize filepath (fromIntegral $ dbSizeBytes sizes)
    (fileStart :: Ptr Word8, rawSize, mmapOffset, _) <- mmapFilePtr filepath ReadWrite Nothing
    when (mmapOffset /= 0) $ errorWithoutStackTrace "we don't trust a non-zero offset from mmap"
    -- putStrLn $ "  fileStart: " ++ show fileStart
    let rodb = Rodb
          { header = castPtr fileStart
          , prefixTable = fileStart `plusPtr` prefixOffsetBytes sizes
          , dataTable = fileStart `plusPtr` dataOffsetBytes sizes
          , rawSize
          , sizes
          }
    -- initialize header and prefix table
    writeHeader rodb
    writePrefixes rodb
    -- done
    pure rodb

insert :: Rodb -> ByteString -> ByteString -> IO ()
insert rodb@Rodb{sizes=Sizes
    {keySizeBytes,prefixSizeBytes,prefixSizeBits,suffixSizeBytes}
  } key val = do
  -- check key size
  when (BS.length key > keySizeBytes) $ do
    errorWithoutStackTrace "provided key larger than key size"
  -- find the appropriate bucket
  let prefix = bytesToWord $ BS.take prefixSizeBytes key
      bucketIx = fromIntegral @Word64 @Int $
                  shiftR prefix (prefixSizeBytes * 8 - prefixSizeBits)
  bounds <- readBucketBounds rodb bucketIx
  -- insertion sort in the bucket
  let suffix = BS.drop (keySizeBytes - suffixSizeBytes) key
  insertToBucket rodb suffix val bounds


calculate :: Config -> BucketSizesElems -> Sizes
calculate Config{keySizeBytes,prefixSizeBits,valSizeBytes} bucketSizes =
  let headerSizeBytes = 32
      offsetSizeBytes = minimalOffsetSizeBytes bucketSizes
      numBuckets = V.length bucketSizes
      prefixSizeBytes = alignTo 8 prefixSizeBits `div` 8
      prefixTableSizeBytes = offsetSizeBytes * (numBuckets + 1)
      suffixSizeBytes = keySizeBytes - (prefixSizeBits `div` 8)
      alignSizeBytes = 0 -- TODO
      rowSizeBytes = suffixSizeBytes + valSizeBytes
      dataTableSizeBytes = (suffixSizeBytes + valSizeBytes) * (fromIntegral $ V.sum bucketSizes)
      dbSizeBytes = headerSizeBytes + prefixTableSizeBytes + alignSizeBytes + dataTableSizeBytes
      prefixOffsetBytes = headerSizeBytes
      dataOffsetBytes = prefixOffsetBytes + prefixTableSizeBytes + alignSizeBytes
   in Sizes
      {keySizeBytes,prefixSizeBits,valSizeBytes
      ,headerSizeBytes,offsetSizeBytes,prefixSizeBytes,prefixTableSizeBytes
      ,suffixSizeBytes,alignSizeBytes,dataTableSizeBytes,dbSizeBytes
      ,prefixOffsetBytes,dataOffsetBytes,bucketSizes,numBuckets,rowSizeBytes
      }

writeHeader :: Rodb -> IO ()
writeHeader Rodb{header,sizes=Sizes
    {keySizeBytes,prefixSizeBits,suffixSizeBytes,offsetSizeBytes,valSizeBytes,dataOffsetBytes}
  } = do
  pokeElemOff header 0 (Fixed 0xb4a10963)
  pokeElemOff header 1 (Fixed $ fromIntegral @Int @Word32 keySizeBytes)
  pokeElemOff header 2 (Fixed $ fromIntegral @Int @Word32 prefixSizeBits)
  pokeElemOff header 3 (Fixed $ fromIntegral @Int @Word32 suffixSizeBytes)
  pokeElemOff header 4 (Fixed $ fromIntegral @Int @Word32 offsetSizeBytes)
  pokeElemOff header 5 (Fixed $ fromIntegral @Int @Word32 valSizeBytes)
  pokeElemOff header 6 (Fixed $ fromIntegral @Int @Word32 dataOffsetBytes)
  pokeElemOff header 7 (Fixed 0)

writePrefixes :: Rodb -> IO ()
writePrefixes Rodb{prefixTable,sizes=Sizes{offsetSizeBytes,bucketSizes}} = loop 0 0
  where
  loop !i !total
    | i < V.length bucketSizes = do
        writePrefix (prefixTable `plusPtr` (i * offsetSizeBytes)) total
        loop (i + 1) (total + bucketSizes V.! i)
    | otherwise = do
        writePrefix (prefixTable `plusPtr` (i * offsetSizeBytes)) total
  writePrefix ptr prefix =
    sequence_ [ pokeByteOff ptr i x
                | i <- [0 .. offsetSizeBytes - 1]
                , let x = fromIntegral @Word64 @Word8 $
                            shiftR prefix (8 * (offsetSizeBytes - (i+1)))
                ]

writeSuffix :: Rodb -> Ptr Word8 -> ByteString -> IO ()
writeSuffix Rodb{sizes=Sizes{suffixSizeBytes}} ptr suffix =
    sequence_ [ pokeByteOff ptr i (BS.index suffix i)
                | i <- [0 .. suffixSizeBytes - 1] ]

readOffset :: Rodb -> Int -> IO Int
readOffset Rodb{prefixTable,sizes=Sizes{offsetSizeBytes}} entry = loop 0 0
  where
  loop !acc !i
    | i < offsetSizeBytes = do
      (r :: Word8) <- peekByteOff prefixTable (entry * offsetSizeBytes + i)
      let acc' = (acc `shiftL` 8) + (fromIntegral r)
      loop acc' (i + 1)
    | otherwise = pure acc

readBucketBounds :: Rodb -> Int -> IO (Int, Int)
readBucketBounds rodb@Rodb{sizes=Sizes{rowSizeBytes}} i = do
  rawStart <- readOffset rodb i
  rawEnd <- readOffset rodb (i + 1)
  pure (rowSizeBytes * rawStart, rowSizeBytes * rawEnd)

insertToBucket :: Rodb -> ByteString -> ByteString -> (Int, Int) -> IO ()
insertToBucket rodb@Rodb{dataTable,sizes=Sizes
    {suffixSizeBytes,valSizeBytes,rowSizeBytes}
  } suffix val (start, end) = do
  -- check value size
  when (BS.length val /= valSizeBytes) $ do
    errorWithoutStackTrace "provided value is not have expected size"
  -- search for the insertion point
  let (PS suffix# suffOff _) = suffix
      startPtr = dataTable `plusPtr` start
      endPtr = dataTable `plusPtr` end
  -- putStrLn $ "   startPtr: " ++ show startPtr
  -- putStrLn $ "     endPtr: " ++ show endPtr
  insertPoint <- withForeignPtr suffix# $ \basePtr -> do
    let needlePtr = basePtr `plusPtr`suffOff
    search needlePtr (startPtr, endPtr)
  let pullPoint = startPtr `plusPtr` rowSizeBytes
      pullSize = (insertPoint `plusPtr` rowSizeBytes) `minusPtr` pullPoint
  -- putStrLn $ "insertPoint: " ++ show insertPoint
  -- putStrLn $ "  pullPoint: " ++ show pullPoint
  -- putStrLn $ "   pullSize: " ++ show pullSize
  -- shuffle memory around to insert
  let (PS val# valOff _) = val
  withForeignPtr val# $ \basePtr -> do
    let valPtr = basePtr `plusPtr` valOff
    moveBytes startPtr pullPoint pullSize
    -- putStrLn "move ok"
    writeSuffix rodb insertPoint suffix
    copyBytes (insertPoint `plusPtr` suffixSizeBytes) valPtr valSizeBytes
    -- putStrLn "copy ok"
  where
  search :: Ptr Word8 -> (Ptr Word8, Ptr Word8) -> IO (Ptr Word8)
  search needle (limit, here0) = go (here0 `plusPtr` (-rowSizeBytes))
    where
    go here
      | here < limit = errorWithoutStackTrace $ "bucket too small (" ++ show here ++ ">=" ++ show limit ++ ")"
      | otherwise = do
        -- putStrLn $ "memcmp here: " ++ show here
        r <- memcmp needle here suffixSizeBytes
        -- putStrLn "memcmp ok"
        if r < 0
          then go (here `plusPtr` (-rowSizeBytes))
          else pure here





------------

minimalOffsetSizeBytes :: BucketSizesElems -> Int
minimalOffsetSizeBytes v =
  let totalEntries = V.sum v
      countZeroBytes = countLeadingZeros totalEntries `div` 8
  in sizeOf totalEntries - countZeroBytes

alignTo :: Int -> Int -> Int
alignTo bound x =
  let r = x `rem` bound
  in if r == 0 then x else x + (bound - r)

-- calculate size of each bucket from input file
-- then create and open the output file
-- write header
-- prefix table is trivial with the bucket sizes
-- read input file, bucketed insertion sort

bytesToWord :: ByteString -> Word64
bytesToWord = loop 0
  where
  loop !acc !bs
    | BS.length bs == 0 = acc
    | otherwise =
      let Just (b, bs') = BS.uncons bs
          acc' = (acc `shiftL` 8) + fromIntegral @Word8 @Word64 b
       in loop acc' bs'