/**
 * Restaurant Service
 * File này chứa các nghiệp vụ liên quan đến nhà hàng
 * Bao gồm transform data từ backend sang frontend format
 */

/**
 * Transform backend restaurant data to frontend format for list view
 */
export function transformRestaurantListItem(restaurant: any) {
    return {
        id: restaurant._id || restaurant.restaurant_id || restaurant.id || '',
        name: restaurant.name || restaurant.restaurant_name || '',
        email: restaurant.email || '',
        address: restaurant.address || '',
        phone: restaurant.hotline || '',
        rating: parseFloat(((restaurant.averageRating || restaurant.average_rating || 0)).toFixed(1)),
        status: restaurant.status ? 'Active' : 'Inactive',
        initial: (restaurant.name || restaurant.restaurant_name || 'R').charAt(0).toUpperCase(),
        colorClass: 'bg-orange-100 text-orange-600',
        reviewsCount: restaurant.totalReviews || restaurant.total_reviews || 0,
        category: restaurant.category || 'Nhà hàng',
    };
}

/**
 * Transform an array of restaurants for list view
 */
export function transformRestaurantsList(restaurants: any[]) {
    return restaurants.map(restaurant => transformRestaurantListItem(restaurant));
}

/**
 * Transform backend restaurant detail data to frontend format
 */
export function transformRestaurantDetail(restaurant: any) {
    const restaurantName = restaurant.name || restaurant.restaurant_name || '';
    const initial = restaurantName.charAt(0).toUpperCase() || 'R';

    // Ensure address is properly extracted
    const address = restaurant.address || '';

    // Ensure menu is properly structured
    let menu = restaurant.menu || [];
    if (!Array.isArray(menu)) {
        console.warn('Menu is not an array, converting:', typeof menu);
        menu = [];
    }

    return {
        id: restaurant._id || restaurant.restaurant_id || restaurant.id || '',
        name: restaurantName,
        email: restaurant.email || '',
        address: address,
        phone: restaurant.hotline || '',
        rating: parseFloat((restaurant.averageRating || restaurant.average_rating || 0).toFixed(1)),
        status: restaurant.status ? 'Active' : 'Inactive',
        menu: menu,
        initial: initial,
        reviewsCount: restaurant.totalReviews || restaurant.total_reviews || 0,
    };
}

/**
 * Transform menu from backend format (array of categories with items) to flat array of food items
 */
export function transformMenuToFoodList(menu: any[]): any[] {
    const foods: any[] = [];

    if (Array.isArray(menu) && menu.length > 0) {
        menu.forEach((category: any, catIdx: number) => {
            const categoryName = category.category || '';
            const items = category.items || [];
            console.log(`Category ${catIdx} (${categoryName}): ${items.length} items`);

            if (Array.isArray(items) && items.length > 0) {
                items.forEach((item: any, itemIdx: number) => {
                    console.log(`  Item ${itemIdx}: ${item.name || 'unnamed'}`);
                    foods.push({
                        id: item.name || `${categoryName}-${item.name}`, // Use name as ID since backend doesn't have food_id
                        name: item.name || '',
                        category: categoryName,
                        price: item.price || 0,
                        description: item.description || '',
                        imageUrl: item.image || item.imageUrl || '',
                        status: item.status !== false ? 'Active' : 'Inactive',
                    });
                });
            }
        });
    }

    return foods;
}
