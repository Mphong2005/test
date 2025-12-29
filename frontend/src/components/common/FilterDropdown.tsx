import React, { useRef, useEffect } from 'react';
import { ChevronDown, X } from 'lucide-react';
import { FilterOption } from '../../types/common';

interface FilterDropdownProps {
    label: string;
    options: FilterOption[];
    selectedValue: string | null;
    onSelect: (value: string | null) => void;
    isOpen: boolean;
    onToggle: () => void;
}

export const FilterDropdown: React.FC<FilterDropdownProps> = ({
    label,
    options,
    selectedValue,
    onSelect,
    isOpen,
    onToggle,
}) => {
    const dropdownRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                if (isOpen) onToggle();
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [isOpen, onToggle]);

    const selectedOption = options.find(opt => opt.value === selectedValue);

    return (
        <div ref={dropdownRef} className="relative">
            <button
                onClick={onToggle}
                className={`bg-white border ${selectedValue ? 'border-[#EE501C] bg-orange-50' : 'border-gray-200'} text-gray-700 px-5 py-2 rounded-xl text-sm font-medium flex items-center gap-2 hover:bg-gray-50 whitespace-nowrap transition-all`}
            >
                {selectedOption ? selectedOption.label : label}
                {selectedValue ? (
                    <X
                        className="w-3 h-3 text-[#EE501C] hover:text-red-600"
                        onClick={(e) => {
                            e.stopPropagation();
                            onSelect(null);
                        }}
                    />
                ) : (
                    <ChevronDown className={`w-3 h-3 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
                )}
            </button>

            {isOpen && (
                <div
                    className="absolute top-full left-0 mt-2 bg-white border border-gray-200 rounded-xl shadow-xl min-w-[180px] overflow-hidden"
                    style={{ zIndex: 9999 }}
                >
                    {options.map((option) => (
                        <button
                            key={option.value}
                            onClick={(e) => {
                                e.stopPropagation();
                                onSelect(option.value);
                                onToggle();
                            }}
                            className={`w-full text-left px-4 py-3 text-sm hover:bg-orange-50 transition-colors ${selectedValue === option.value ? 'bg-orange-50 text-[#EE501C] font-semibold' : 'text-gray-700'
                                }`}
                        >
                            {option.label}
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
};
