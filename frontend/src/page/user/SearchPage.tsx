import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { SearchOverlay } from '../../components/user/SearchOverlay';
import { SearchResults } from '../../components/user/SearchResults';
import { FoodItem } from '../../types/common';
import { getFoodsApi } from '../../api/productApi';
import { addToCartApi, getCartApi, removeFromCartApi } from '../../api/cartApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { extractErrorMessage } from '../../utils';

export const SearchPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { isAuthenticated } = useAuthContext();
  const [searchTerm, setSearchTerm] = useState('');
  const [showResults, setShowResults] = useState(false);
  const [favoriteIds, setFavoriteIds] = useState<string[]>([]);
  const [foods, setFoods] = useState<FoodItem[]>([]);

  // Load foods
  useEffect(() => {
    const fetchFoods = async () => {
      try {
        const data = await getFoodsApi();
        setFoods(data);
      } catch (error) {
        console.error("Failed to fetch foods", error);
      }
    };
    fetchFoods();
  }, []);

  useEffect(() => {
    // If navigation state contains a query, show results
    if (location.state && location.state.query) {
      setSearchTerm(location.state.query);
      setShowResults(true);
    } else {
      // Otherwise, reset to initial overlay state
      setSearchTerm('');
      setShowResults(false);
    }
  }, [location.state]);

  const handleSearch = (term: string) => {
    // Navigate with new query to keep URL/History and Header in sync
    navigate('/search', { state: { query: term } });
  };

  const handleProductClick = (item: FoodItem) => {
    navigate(`/product/${item.id}`);
  };

  // Load favorites from cart on mount
  useEffect(() => {
    const loadFavorites = async () => {
      if (isAuthenticated) {
        try {
          const cart = await getCartApi();
          if (cart && cart.items) {
            // Extract food IDs from cart items (format: restaurantId-foodName)
            const cartFoodIds = cart.items.map((item: any) => {
              return `${item.restaurantId}-${item.foodName}`;
            });
            setFavoriteIds(cartFoodIds);
          }
        } catch (error) {
          console.error('Failed to load favorites from cart:', error);
        }
      }
    };
    loadFavorites();
  }, [isAuthenticated]);

  const handleToggleFavorite = async (id: string) => {
    // Update local state
    const isAdding = !favoriteIds.includes(id);
    setFavoriteIds(prev =>
      prev.includes(id) ? prev.filter(item => item !== id) : [...prev, id]
    );

    // Save to cart (favorites) in backend if authenticated
    if (isAuthenticated) {
      try {
        const food = foods.find(f => f.id === id);
        if (food && food.restaurantId) {
          // Extract food name from food.id format: restaurantId-foodName
          const foodName = food.id.includes('-') ? food.id.split('-').slice(1).join('-') : food.name;

          if (isAdding) {
            // Add to cart (favorites)
            await addToCartApi(food.restaurantId, foodName, 1);
          } else {
            // Remove from cart (favorites)
            await removeFromCartApi(foodName);
          }
        }
      } catch (error: unknown) {
        console.error('Failed to update favorites:', error);
        // Revert local state on error
        setFavoriteIds(prev =>
          prev.includes(id) ? prev.filter(item => item !== id) : [...prev, id]
        );
        alert(extractErrorMessage(error));
      }
    }
  };

  return (
    <div className="min-h-screen bg-white pb-20">
      <div className="max-w-[1280px] mx-auto pt-4">
        {!showResults ? (
          <SearchOverlay
            onSearch={handleSearch}
            searchValue={searchTerm}
            onSearchChange={setSearchTerm}
          />
        ) : (
          <SearchResults
            searchTerm={searchTerm}
            onItemClick={handleProductClick}
            favoriteFoodIds={favoriteIds}
            onToggleFavorite={handleToggleFavorite}
            foods={foods} // Pass loaded foods
          />
        )}
      </div>
    </div>
  );
};