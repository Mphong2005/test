/**
 * Date Helpers
 * Utility functions for date parsing, formatting, and grouping
 */

/**
 * Parse order time string "HH:mm - DD/MM/YYYY" to Date object
 */
export const parseOrderDate = (timeString: string): Date | null => {
    try {
        if (timeString.includes('-')) {
            const datePart = timeString.split('-')[1].trim(); // "DD/MM/YYYY"
            const dateParts = datePart.split('/').map(Number);
            if (dateParts.length === 3) {
                // Format: DD/MM/YYYY
                return new Date(dateParts[2], dateParts[1] - 1, dateParts[0]);
            } else if (dateParts.length === 2) {
                // Format: DD/MM (assume current year)
                return new Date(new Date().getFullYear(), dateParts[1] - 1, dateParts[0]);
            }
        }
        return null;
    } catch {
        return null;
    }
};

/**
 * Check if two dates are the same day
 */
export const isSameDate = (d1: Date, d2: Date): boolean =>
    d1.getDate() === d2.getDate() &&
    d1.getMonth() === d2.getMonth() &&
    d1.getFullYear() === d2.getFullYear();

/**
 * Format date as "DD Tháng MM, YYYY" (Vietnamese style)
 */
export const formatDateLabelVN = (date: Date): string => {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear();
    return `${day} Tháng ${month}, ${year}`;
};

/**
 * Group interface for date-grouped data
 */
export interface DateGroup<T> {
    key: string;
    label: string;
    displayDate: string;
    items: T[];
    timestamp: number;
}

/**
 * Group items by date with today/yesterday labels
 * @param items - Array of items to group
 * @param getTimeString - Function to extract time string from item
 * @returns Sorted array of date groups
 */
export const groupByDate = <T>(
    items: T[],
    getTimeString: (item: T) => string
): DateGroup<T>[] => {
    const groups: Record<string, DateGroup<T>> = {};
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    items.forEach(item => {
        const parsedDate = parseOrderDate(getTimeString(item));
        const itemDate = parsedDate || new Date();

        let groupKey = '';
        let label = '';
        let displayDate = '';

        if (isSameDate(itemDate, today)) {
            groupKey = 'today';
            label = 'Hôm nay';
            displayDate = formatDateLabelVN(itemDate);
        } else if (isSameDate(itemDate, yesterday)) {
            groupKey = 'yesterday';
            label = 'Hôm qua';
            displayDate = formatDateLabelVN(itemDate);
        } else {
            groupKey = itemDate.toISOString().split('T')[0];
            label = '';
            displayDate = formatDateLabelVN(itemDate);
        }

        if (!groups[groupKey]) {
            groups[groupKey] = {
                key: groupKey,
                label,
                displayDate,
                items: [],
                timestamp: itemDate.getTime()
            };
        }
        groups[groupKey].items.push(item);
    });

    // Sort by date descending, with today and yesterday first
    return Object.values(groups).sort((a, b) => {
        if (a.label === 'Hôm nay') return -1;
        if (b.label === 'Hôm nay') return 1;
        if (a.label === 'Hôm qua') return -1;
        if (b.label === 'Hôm qua') return 1;
        return b.timestamp - a.timestamp;
    });
};

/**
 * Get relative date label (Hôm nay, Hôm qua, or formatted date)
 */
export const getRelativeDateLabel = (date: Date): { label: string; displayDate: string } => {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    if (isSameDate(date, today)) {
        return { label: 'Hôm nay', displayDate: formatDateLabelVN(date) };
    } else if (isSameDate(date, yesterday)) {
        return { label: 'Hôm qua', displayDate: formatDateLabelVN(date) };
    } else {
        return { label: '', displayDate: formatDateLabelVN(date) };
    }
};
