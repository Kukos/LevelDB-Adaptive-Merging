#ifndef BASE_ENCODER_HPP
#define BASE_ENCODER_HPP

#include <string>

class BaseEncoder
{
private:
    static size_t base64CharPos(unsigned char chr) noexcept(true);

public:
    static std::string base64Encode(const unsigned char* str, size_t len) noexcept(true);
    static std::string base64Decode(const std::string& s) noexcept(true);
};

#endif
