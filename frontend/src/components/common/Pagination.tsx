import React from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';

interface PaginationProps {
  currentPage: number;
  totalPages: number;
  startIndex: number;
  endIndex: number;
  totalItems: number;
  onPageChange: (page: number) => void;
  label?: string;
}

export const Pagination: React.FC<PaginationProps> = ({
  currentPage,
  totalPages,
  startIndex,
  endIndex,
  totalItems,
  onPageChange,
  label = 'mục'
}) => {
  const hasPrevPage = currentPage > 1;
  const hasNextPage = currentPage < totalPages;

  return (
    <div className="bg-white px-6 py-4 flex items-center justify-between border-t border-gray-200">
      <div className="text-sm text-gray-500">
        Hiển thị <span className="font-bold text-gray-900">{totalItems > 0 ? startIndex : 0}</span> đến <span className="font-bold text-gray-900">{endIndex}</span> của <span className="font-bold text-gray-900">{totalItems}</span> {label}
      </div>

      <div className="flex items-center border border-gray-200 rounded-lg overflow-hidden divide-x divide-gray-200 shadow-sm">
        <button
          onClick={() => onPageChange(currentPage - 1)}
          disabled={!hasPrevPage}
          className="px-2.5 py-2 text-gray-400 hover:bg-gray-50 transition-colors disabled:opacity-30 disabled:cursor-not-allowed bg-white h-full flex items-center justify-center w-10"
        >
          <ChevronLeft size={18} />
        </button>
        <div className="px-4 py-2 text-sm text-gray-700 font-medium bg-white min-w-[120px] text-center h-full flex items-center justify-center">
          Trang {currentPage} / {totalPages || 1}
        </div>
        <button
          onClick={() => onPageChange(currentPage + 1)}
          disabled={!hasNextPage}
          className="px-2.5 py-2 text-gray-400 hover:bg-gray-50 transition-colors disabled:opacity-30 disabled:cursor-not-allowed bg-white h-full flex items-center justify-center w-10"
        >
          <ChevronRight size={18} />
        </button>
      </div>
    </div>
  );
};