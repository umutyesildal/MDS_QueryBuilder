"""
Unified Configuration System for MIMIC-IV Medallion Pipeline
===========================================================

This module provides a centralized configuration system that consolidates
all the scattered config files into a clean, environment-aware setup.

Usage:
    from src.config import get_config
    config = get_config('development')  # or 'testing', 'production'
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = 'localhost'
    port: int = 5432
    database: str = 'mimiciv'
    user: str = 'postgres'
    password: Optional[str] = None
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        if self.password:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

@dataclass
class PipelineConfig:
    """ETL Pipeline configuration"""
    # Layer configurations
    bronze_schema: str = 'bronze'
    silver_schema: str = 'silver' 
    gold_schema: str = 'gold'
    
    # Processing settings
    batch_size: int = 10000
    parallel_workers: int = 4
    checkpoint_enabled: bool = True
    
    # Data quality settings
    missing_threshold: float = 0.7  # Allow up to 70% missing data
    outlier_removal: bool = True
    
@dataclass
class ScoringConfig:
    """SOFA scoring configuration for Task 5.4"""
    
    # Configuration 1: Mean-based aggregation
    config_1 = {
        'name': 'mean_based',
        'aggregation_method': 'mean',
        'time_window_hours': 24,
        'table_name': 'gold_scores_config1',
        'imputation_strategy': 'mean',
        'outlier_method': 'iqr'
    }
    
    # Configuration 2: Median-based aggregation  
    config_2 = {
        'name': 'median_based',
        'aggregation_method': 'median',
        'time_window_hours': 12,
        'table_name': 'gold_scores_config2', 
        'imputation_strategy': 'median',
        'outlier_method': 'percentile'
    }

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = 'INFO'
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    file_path: str = 'logs/pipeline.log'
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

class Settings:
    """Main settings class that combines all configurations"""
    
    def __init__(self, environment: str = 'development'):
        self.environment = environment
        self.database = self._get_database_config()
        self.pipeline = PipelineConfig()
        self.scoring = ScoringConfig()
        self.logging = LoggingConfig()
        
        # Load OMOP concepts
        self.omop_concepts = self._load_omop_concepts()
        
    def _get_database_config(self) -> DatabaseConfig:
        """Load database configuration based on environment"""
        
        # Try to load from local config first
        try:
            from config_local import DB_CONFIG
            return DatabaseConfig(**DB_CONFIG)
        except ImportError:
            pass
            
        # Environment-specific defaults
        if self.environment == 'testing':
            return DatabaseConfig(
                database='mimiciv_test',
                user='test_user'
            )
        elif self.environment == 'production':
            return DatabaseConfig(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME', 'mimiciv'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD')
            )
        else:  # development
            return DatabaseConfig()
    
    def _load_omop_concepts(self) -> Dict[str, int]:
        """Load OMOP concept mappings"""
        return {
            'PaO2_FiO2_Ratio': 40762499,
            'SpO2_FiO2_Ratio': 40764520,
            'Respiratory_Rate': 3027018,
            'Mean_Arterial_Pressure': 4154790,
            'Systolic_Blood_Pressure': 4152194,
            'Diastolic_Blood_Pressure': 4154790,
            'Heart_Rate': 4239408,
            'Temperature': 4302666,
            'Glasgow_Coma_Scale': 4216540,
            'Platelets': 4267147,
            'Bilirubin': 4146449,
            'Creatinine': 4134493,
            'Urine_Output': 4267392,
            'Vasopressor_Use': 21602796,
            'Mechanical_Ventilation': 4230460
        }

# Global settings instance
_settings: Optional[Settings] = None

def get_config(environment: str = None) -> Settings:
    """
    Get the global configuration instance
    
    Args:
        environment: 'development', 'testing', or 'production'
        
    Returns:
        Settings instance
    """
    global _settings
    
    if _settings is None or (environment and _settings.environment != environment):
        env = environment or os.getenv('ENVIRONMENT', 'development')
        _settings = Settings(env)
    
    return _settings

def reset_config():
    """Reset the global configuration (useful for testing)"""
    global _settings
    _settings = None

# Convenience functions for backward compatibility
def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return get_config().database

def get_pipeline_config() -> PipelineConfig:
    """Get pipeline configuration"""
    return get_config().pipeline

def get_scoring_config() -> ScoringConfig:
    """Get scoring configuration"""
    return get_config().scoring

# Legacy support - maintain compatibility with existing imports
DB_CONFIG = get_database_config().__dict__
CONFIG_1 = get_scoring_config().config_1
CONFIG_2 = get_scoring_config().config_2
