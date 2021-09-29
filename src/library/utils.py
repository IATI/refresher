import chardet
from library.logger import getLogger

logger = getLogger()

def get_text_from_blob(downloader, file_hash):  
    # save off bytes if we need to detect charset later
    downloadBytes = downloader.content_as_bytes()
    try:
        return downloader.content_as_text()
    except UnicodeDecodeError:
        logger.info('File is not UTF-8, trying to detect encoding for file with hash ' + file_hash)
        pass
    
    # If not UTF-8 try to detect charset and decode
    try:
        detect_result = chardet.detect(downloadBytes)
        charset = detect_result['encoding']
        if charset:
            logger.info('Charset detected: ' + charset + ' Confidence: ' + str(detect_result['confidence']) + ' Language: ' + detect_result['language'] + ' for file with hash ' + file_hash)
            return downloader.content_as_text(encoding=charset)
        logger.warning('No Charset detected for file with hash ' + file_hash + '. Likely a non-text file.')
        raise
    except:
        logger.warning('Could not determine charset to decode for file with hash ' + file_hash)
        raise
    