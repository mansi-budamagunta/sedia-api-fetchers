#!/usr/bin/env python3
"""
SEDIA Data Retrieval
====================================
Extraction pipeline for fetching project data from ALL programmes

Includes comprehensive logging, change detection, and performance monitoring.
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from sedia_api_fetchers.database import DatabaseSink
from dotenv import load_dotenv
from sqlalchemy import text

# Add current directory to path for imports
script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(script_dir))

# Ensure that the db_url is set
load_dotenv(script_dir / ".env")
db_url = os.environ.get("SEDIA_DB_URL")
if not db_url:
    # print(db_url)
    raise RuntimeError(
        "Missing required environment variable SEDIA_DB_URL. "
        "Set it to the SQLAlchemy database connection URL before running the projects-ingestion pipeline."
    )

@dataclass
class ETLConfig:
    """Configuration for the ETL pipeline."""
    logs_dir: Path
    db_url: str = field(repr=False)
    projects_staging_table: str = "raw_projects_staging"
    projects_published_table: str = "raw_projects"
    min_record_threshold: int = 100
    log_level: str = "INFO"

@dataclass
class ProgrammeMetadata:
    """Metadata for a programme."""
    id: int
    name: str
    record_count: int

@dataclass
class ExtractionResult:
    """Result of data extraction."""
    programme: ProgrammeMetadata
    endpoint: str
    extraction_time: float
    success: bool
    record_count: int = 0
    error_message: Optional[str] = None

class ETLLogger:
    """Centralized logging for the ETL pipeline."""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.setup_logging()
    
    def setup_logging(self):
        """Set up main logging."""
        self.config.logs_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.config.logs_dir / f"etl_pipeline_{timestamp}.log"
        
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.main_logger = logging.getLogger('etl_pipeline')
        self.log_file = log_file
    
class ETLExtractor:
    """Extract phase - Fetch raw data from SEDIA APIs."""
    
    def __init__(
        self,
        logger: ETLLogger,
        project_sink: DatabaseSink | None = None,
    ):
        self.logger = logger
        self.main_logger = logger.main_logger
        self.project_sink = project_sink
        self._import_fetchers()

    def _import_fetchers(self):
        """Import all fetcher classes."""
        try:
            from sedia_api_fetchers.EUFT_retrieve_projects import SEDIA_GET_PROJECTS
            from sedia_api_fetchers.EUFT_retrieve_facets import SEDIA_GET_FACETS
            
            self.projects_fetcher = SEDIA_GET_PROJECTS

            self.facet_fetcher = SEDIA_GET_FACETS

            self.main_logger.info("All fetcher classes imported successfully")
        except ImportError as e:
            self.main_logger.error(f"Failed to import fetchers: {e}")
            raise
    
    def extract_programme_metadata(self, config: ETLConfig) -> List[ProgrammeMetadata]:
        """Extract programme metadata from facet data."""
        self.main_logger.info("EXTRACT PHASE: Fetching current programme metadata from facets API")

        start_time = time.time()
        facet_data = self.facet_fetcher(flatten_metadata=False).get(save=False)

        if not facet_data:
            raise RuntimeError("No facet data returned by the facets API")
        
        programmes = []
        for facet in facet_data.get('facets', []):
            if facet.get('name') == 'programId':
                for prog in facet.get('values', []):
                    programme_id = int(prog['rawValue'])
                    programme_name = prog['value']
                    record_count = prog['count']
                    
                    programmes.append(ProgrammeMetadata(
                        id=programme_id,
                        name=programme_name,
                        record_count=record_count
                    ))
                # Once the 'programId' facet is found & processed, exit the outer loop to skip the rest of the facets
                break
        
        # Sort by record count (descending)
        # programmes.sort(key=lambda x: x.record_count, reverse=True)
        
        # Sort by record count (ascending)
        programmes.sort(key=lambda x: x.record_count)

        extraction_time = time.time() - start_time
        self.main_logger.info(f"Extracted {len(programmes)} programmes in {extraction_time:.1f}s")
        
        return programmes
    
    def extract_programme_data(self, programme: ProgrammeMetadata, endpoint: str) -> ExtractionResult:
        """Extract data for a specific programme and endpoint."""
        start_time = time.time()
        self.main_logger.info(f"EXTRACT: {endpoint} for {programme.name} (ID: {programme.id})")
        
        # Check if this programme might exceed API limits
        if programme.record_count > 10000:
            self.main_logger.warning(f"Large dataset detected ({programme.record_count:,} records) - using date partitioning strategy")
        
        try:
            fetcher_class = self.projects_fetcher

            if self.project_sink is None:
                raise RuntimeError("Project SQL database_sink is not configured")

            fetcher = fetcher_class(
                flatten_metadata=True,
                enrich_with_details=False,
            )

            rows_before = self.project_sink.rows_written

            fetcher.fetch_all_records(
                programme.id,
                database_sink=self.project_sink,
                return_df=False,
            )

            rows_written = self.project_sink.rows_written - rows_before
            extraction_time = time.time() - start_time

            if rows_written > 0:
                self.main_logger.info(
                    f"EXTRACT SUCCESS: streamed {rows_written:,} project records "
                    f"in {extraction_time:.1f}s"
                )

                return ExtractionResult(
                    programme=programme,
                    endpoint=endpoint,
                    extraction_time=extraction_time,
                    success=True,
                    record_count=rows_written,
                )

            self.main_logger.warning(
                f"EXTRACT WARNING: No project data returned for {programme.name}"
            )

            return ExtractionResult(
                programme=programme,
                endpoint=endpoint,
                extraction_time=extraction_time,
                success=False,
                record_count=0,
                error_message="No project data returned",
            )

        except Exception as e:
            extraction_time = time.time() - start_time
            partial_rows = (
                self.project_sink.rows_written
                if endpoint == "projects" and self.project_sink is not None
                else 0
            )

            self.main_logger.error(
                f"EXTRACT FAILED: {endpoint} for {programme.name} "
                f"after {extraction_time:.1f}s: {e}"
            )

            return ExtractionResult(
                programme=programme,
                endpoint=endpoint,
                extraction_time=extraction_time,
                success=False,
                record_count=partial_rows,
                error_message=str(e),
            )

class ETLPipeline:
    """Main ETL Pipeline orchestrator."""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = ETLLogger(config)

        self.project_sink = DatabaseSink(
            config.db_url,
            config.projects_staging_table,
        )

        self.extractor = ETLExtractor(
            self.logger,
            project_sink=self.project_sink,
        )
        
        self.main_logger = self.logger.main_logger

    def check_database_connection(self):
        """Confirm database availability before making API requests."""
        self.main_logger.info("Checking database connection")
        with self.project_sink.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        self.main_logger.info("Database connection successful")


    def programme_needs_project_fetch(
        self,
        programme: ProgrammeMetadata,
    ) -> bool:
        """Return whether published projects are fewer than the facet count."""
        published_count = self.project_sink.count_programme_rows(
            programme.id,
            self.config.projects_published_table,
        )

        if published_count > programme.record_count:
            self.main_logger.warning(
                f"Published project count exceeds facet count for "
                f"{programme.name}: "
                f"P={published_count}, F={programme.record_count}"
            )
            return False

        return published_count < programme.record_count

    
    def run(self) -> Dict[str, Any]:
        """Execute the complete ETL pipeline."""
        pipeline_start_time = time.time()
        
        self.main_logger.info("="*80)
        self.main_logger.info("ETL PIPELINE STARTING")
        self.main_logger.info("="*80)
        self.main_logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.main_logger.info(f"Configuration: {self.config}")
        
        # Initialize results tracking
        results = {
            'programmes_processed': 0,
            'total_records': 0,
            'extraction_results': [],
            'timing': {
                'extraction': 0,
                'total': 0
            }
        }
        
        try:
            # EXTRACT: Get programme metadata
            programmes = self.extractor.extract_programme_metadata(self.config)
            major_programmes = [p for p in programmes if p.record_count >= self.config.min_record_threshold]

            self.check_database_connection()
            
            self.main_logger.info(f"Processing {len(major_programmes)} programmes (>={self.config.min_record_threshold} records) out of {len(programmes)} total programmes")
            
            # Process each programme
            for i, programme in enumerate(major_programmes, 1):
                self.main_logger.info("="*60)
                self.main_logger.info(f"PROGRAMME {i}/{len(major_programmes)}: {programme.name}")
                self.main_logger.info(f"Expected records: {programme.record_count:,}")
                self.main_logger.info("="*60)
                    
                if not self.programme_needs_project_fetch(programme):
                    self.main_logger.info(
                        f"Skipping projects for {programme.name}: "
                        "published count satisfies facet count"
                    )
                    continue

                # Drop the current staging table
                self.project_sink.start_fresh_load(programme)

                # EXTRACT
                extraction_result = self.extractor.extract_programme_data(
                    programme,
                    'projects',
                )
                results["extraction_results"].append(extraction_result)
                results["timing"]["extraction"] += (
                    extraction_result.extraction_time
                )

                if extraction_result.record_count > 0:

                    self.main_logger.info(
                        f"Publishing {extraction_result.record_count:,} staged projects "
                        f"for {programme.name}"
                    )
                    publish_start = time.time()

                    publish_result = self.project_sink.publish_programme_staging(
                        self.config.projects_staging_table,
                        self.config.projects_published_table,
                    )

                    publish_time = time.time() - publish_start

                    staged_count = publish_result["staged"]
                    status = (
                        "complete"
                        if staged_count == programme.record_count
                        else "partial"
                    )

                    self.main_logger.info(
                        f"Project publish for {programme.name}: "
                        f"S={staged_count}, F={programme.record_count}, "
                        f"inserted={publish_result['inserted']}, "
                        f"invalid={publish_result['invalid']}, "
                        f"duplicates={publish_result['duplicate_staged']}, "
                        f"P={publish_result['published']} ({status}) in {publish_time:.1f}s"
                    )

                    results["total_records"] += (
                        extraction_result.record_count
                    )

                results['programmes_processed'] += 1
            
        except Exception as e:
            self.main_logger.error(f"Pipeline failed: {e}")
            raise
        
        finally:
            # Calculate final timing
            results['timing']['total'] = time.time() - pipeline_start_time
            
            # Log final summary
            self._log_final_summary(results)
        
        return results
    
    def _log_final_summary(self, results: Dict[str, Any]):
        """Log comprehensive pipeline summary."""
        self.main_logger.info("="*80)
        self.main_logger.info("ETL PIPELINE COMPLETED")
        self.main_logger.info("="*80)
        
        # Performance summary
        timing = results['timing']
        self.main_logger.info("PERFORMANCE SUMMARY:")
        self.main_logger.info(f"   Total execution time: {timing['total']:.1f}s")
        self.main_logger.info(f"   Extraction time: {timing['extraction']:.1f}s ({timing['extraction']/timing['total']*100:.1f}%)")
        
        # Data summary
        self.main_logger.info("DATA SUMMARY:")
        self.main_logger.info(f"   Programmes processed: {results['programmes_processed']}")
        self.main_logger.info(f"   Total records: {results['total_records']:,}")

        # Success rates
        successful_extractions = sum(
            1 for r in results["extraction_results"] if r.success
        )

        extraction_total = len(results["extraction_results"])

        extraction_rate = (
            f"{successful_extractions / extraction_total * 100:.1f}%"
            if extraction_total
            else "n/a"
        )

        self.main_logger.info("SUCCESS RATES:")
        self.main_logger.info(
            f"   Extraction success: "
            f"{successful_extractions}/{extraction_total} "
            f"({extraction_rate})"
        )

        self.main_logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.main_logger.info(f"Log file: {self.logger.log_file}")

def main():
    """Main entry point for the ETL pipeline."""
    # Set up configuration
    base_dir = script_dir
    config = ETLConfig(
        logs_dir=base_dir / "logs",
        db_url=db_url,
        min_record_threshold=1,
        log_level="INFO",
    )
    
    # Ensure directories exist
    config.logs_dir.mkdir(exist_ok=True)
    
    # Run the ETL pipeline
    pipeline = ETLPipeline(config)
    pipeline.run()

if __name__ == "__main__":
    main()