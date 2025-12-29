
import React, { KeyboardEvent, useState, useEffect } from 'react';
import { Clock, X, TrendingUp, Search as SearchIcon, ArrowRight, Trash2 } from 'lucide-react';
import { getSuggestionsApi, getSearchHistoryApi, addSearchHistoryApi, clearSearchHistoryApi } from '../../api/productApi';

interface SearchProps {
  onSearch: (term: string) => void;
  searchValue: string;
  onSearchChange: (val: string) => void;
}

export const SearchOverlay: React.FC<SearchProps> = ({ onSearch, searchValue, onSearchChange }) => {
  const [history, setHistory] = useState<string[]>([]);
  const [suggestions, setSuggestions] = useState<any[]>([]);

  // Load initial data
  useEffect(() => {
    const fetchData = async () => {
      const [historyData, suggestionsData] = await Promise.all([
        getSearchHistoryApi(),
        getSuggestionsApi()
      ]);
      setHistory(historyData);
      setSuggestions(suggestionsData);
    };
    fetchData();
  }, []);

  const handleSearchTrigger = async (term: string) => {
    // Add to history store
    const newHistory = await addSearchHistoryApi(term);
    setHistory(newHistory);
    onSearch(term);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && searchValue.trim()) {
      handleSearchTrigger(searchValue);
    }
  };

  const handleRemoveHistoryItem = (e: React.MouseEvent, term: string) => {
    e.stopPropagation();
    // In real app, we might need a specific remove API, for now filter local visual
    setHistory(prev => prev.filter(item => item !== term));
  };

  const handleClearAllHistory = async () => {
    await clearSearchHistoryApi();
    setHistory([]);
  };

  return (
    <div className="max-w-4xl mx-auto px-6 py-8 animate-in fade-in slide-in-from-top-4 duration-300">
      {/* Search Input Section */}
      <section className="mb-10">
        <div className="relative group">
          <input
            type="text"
            autoFocus
            placeholder="Bạn muốn ăn gì hôm nay?"
            value={searchValue}
            onChange={(e) => onSearchChange(e.target.value)}
            onKeyDown={handleKeyDown}
            className="w-full bg-white border-2 border-orange-50 rounded-[1.5rem] py-4 px-12 shadow-md focus:border-[#EE501C] focus:ring-4 focus:ring-orange-50 transition-all outline-none text-base placeholder:text-gray-400"
          />
          <SearchIcon className="absolute left-4 top-1/2 -translate-y-1/2 text-[#EE501C] w-5 h-5" />
          {searchValue && (
            <button
              onClick={() => handleSearchTrigger(searchValue)}
              className="absolute right-2 top-1/2 -translate-y-1/2 bg-[#EE501C] text-white p-2 rounded-xl hover:bg-[#d44719] transition-colors"
            >
              <ArrowRight className="w-5 h-5" />
            </button>
          )}
        </div>
      </section>

      {/* History */}
      {history.length > 0 && (
        <section className="mb-10">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2 text-gray-800 font-bold">
              <Clock className="w-4 h-4 text-orange-500" />
              <span>Lịch sử tìm kiếm</span>
            </div>
            <button
              onClick={handleClearAllHistory}
              className="text-xs font-bold text-orange-500 hover:text-orange-700 flex items-center gap-1 transition-colors"
            >
              <Trash2 className="w-3 h-3" /> Xóa tất cả
            </button>
          </div>
          <div className="flex flex-wrap gap-3">
            {/* Limit display to 5 most recent items for a cleaner look */}
            {history.slice(0, 5).map((term, idx) => (
              <div
                key={`${term}-${idx}`}
                onClick={() => handleSearchTrigger(term)}
                className="flex items-center gap-2 bg-gray-100 px-4 py-2 rounded-full cursor-pointer hover:bg-orange-50 group transition-all"
              >
                <Clock className="w-3 h-3 text-gray-400 group-hover:text-orange-400" />
                <span className="text-sm text-gray-700 group-hover:text-orange-700 font-medium">{term}</span>
                <button
                  onClick={(e) => handleRemoveHistoryItem(e, term)}
                  className="p-0.5 rounded-full hover:bg-gray-200 text-gray-400 hover:text-red-500 transition-all"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Suggestions */}
      <section>
        <div className="flex items-center gap-2 text-gray-800 font-bold mb-6">
          <TrendingUp className="w-4 h-4 text-orange-500" />
          <span>Gợi ý cho bạn</span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          {suggestions.slice(0, 8).map((item, idx) => (
            <div
              key={idx}
              onClick={() => handleSearchTrigger(item.name)}
              className="bg-white p-3 rounded-2xl border border-gray-100 shadow-sm flex flex-col items-center gap-3 hover:shadow-md hover:border-orange-100 cursor-pointer transition-all group"
            >
              <div className="w-16 h-16 md:w-20 md:h-20 shrink-0 rounded-xl overflow-hidden">
                <img src={item.image} alt={item.name} className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500" />
              </div>
              <div className="text-center min-w-0 w-full">
                <h4 className="font-bold text-sm text-gray-800 group-hover:text-orange-600 transition-colors truncate">{item.name}</h4>
                <p className="text-[10px] text-gray-400 mt-0.5 truncate">{item.tag}</p>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
};
