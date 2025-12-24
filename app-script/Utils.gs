function parsePeriod(period) {
  if (period instanceof Date) {
    return {
      month: period.getMonth() + 1,
      year: period.getFullYear(),
    };
  }

  const months = {
    Jan: 1,
    Feb: 2,
    Mar: 3,
    Apr: 4,
    May: 5,
    Jun: 6,
    Jul: 7,
    Aug: 8,
    Sep: 9,
    Oct: 10,
    Nov: 11,
    Dec: 12,
  };

  const match = String(period).match(/(\w+)\s+(\d{4})/);
  if (!match) return {};

  return {
    month: months[match[1]] || null,
    year: parseInt(match[2]),
  };
}

function formatDate(value) {
  if (value instanceof Date) {
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, '0');
    const day = String(value.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
  return String(value);
}
