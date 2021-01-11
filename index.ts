import { Qiniu, KODO_MODE, S3_MODE } from './qiniu';
import { DEFAULT_UC_URL, Region } from './region';
import { RegionService } from './region_service';
import { Uploader } from './uploader';
import { Downloader } from './downloader';

module.exports = {
    Qiniu, KODO_MODE, S3_MODE, DEFAULT_UC_URL, Region, RegionService, Uploader, Downloader,
};
