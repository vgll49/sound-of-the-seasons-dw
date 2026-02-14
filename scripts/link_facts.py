from services.fact_linker import FactLinker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Linking facts to dimensions...")
    
    linker = FactLinker()
    linker.link_weather_to_facts()
    
    logger.info("Facts linked")

if __name__ == "__main__":
    main()