# EVE Online Market Database Manager

A Python-based database management system for EVE Online market data, built to handle market orders, history, statistics, and doctrine-related information.

## Features

- Market data management for EVE Online
- Handles multiple data types:
  - Market Orders
  - Market History
  - Market Statistics
  - Ship Doctrines
  - Ship Targets
  - Doctrine Maps
  - Doctrine Fits
  - Lead Ships
- Automatic database backups
- Data integrity checks
- Error handling and recovery
- Chunked data processing for large datasets

## Prerequisites

- Python 3.x
- Required Python packages:
  - pandas
  - sqlalchemy
  - libsql_experimental
  - python-dotenv
  - numpy

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with the following variables:
```
FLY_WCMKT_URL=your_market_url
FLY_WCMKT_TOKEN=your_market_token
FLY_SDE_URL=your_sde_url
FLY_SDE_TOKEN=your_sde_token
```

## Usage

The script can be run with various command-line arguments:

```bash
# Update all market data
python turso_handler.py

# Update history data
python turso_handler.py --hist

# Update doctrine fits
python turso_handler.py --update_fits

# Update lead ships
python turso_handler.py --update_lead_ships
```

## Database Structure

The system manages several tables:
- market_history
- marketorders
- marketstats
- doctrines
- ship_targets
- doctrine_map
- doctrine_fits
- lead_ships

## Backup System

- Automatic backups are created before major operations
- Backups are stored in the configured backup directory
- Old backups are automatically cleaned up (keeps last 3 backups)

## Error Handling

- Comprehensive error logging
- Automatic retry mechanism for failed operations
- Database restoration from backups on failure
- Detailed reporting of successful and failed operations

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Acknowledgments

- EVE Online for providing the market data
- The EVE Online community for their support and feedback 