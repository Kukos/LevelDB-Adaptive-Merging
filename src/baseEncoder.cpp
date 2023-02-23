#include <baseEncoder.hpp>
#include <logger.hpp>

#include <algorithm>

size_t BaseEncoder::base64CharPos(const unsigned char chr) noexcept(true)
{
   if (chr >= 'A' && chr <= 'Z')
      return chr - 'A';
   else if (chr >= 'a' && chr <= 'z')
      return chr - 'a' + ('Z' - 'A') + 1;
   else if (chr >= '0' && chr <= '9')
      return chr - '0' + ('Z' - 'A') + ('z' - 'a') + 2;
   else if (chr == '+' || chr == '-')
      return 62;
   else if (chr == '/' || chr == '_')
      return 63;
   else
      return 0;
}

std::string BaseEncoder::base64Encode(unsigned char const* str, const size_t len) noexcept(true)
{
   if (str == nullptr || len == 0)
      return std::string("");

   constexpr char trailing_char = '=';
   const char base64_charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

   std::string ret;
   ret.reserve((len + 2) / 3 * 4);

   for (size_t pos = 0; pos < len; pos += 3)
   {
      ret.push_back(base64_charset[(str[pos + 0] & 0xfc) >> 2]);
      if (pos + 1 < len)
      {
         ret.push_back(base64_charset[((str[pos] & 0x03) << 4) + ((str[pos + 1] & 0xf0) >> 4)]);

         if (pos + 2 < len)
         {
            ret.push_back(base64_charset[((str[pos + 1] & 0x0f) << 2) + ((str[pos + 2] & 0xc0) >> 6)]);
            ret.push_back(base64_charset[str[pos + 2] & 0x3f]);
         }
         else
         {
            ret.push_back(base64_charset[(str[pos + 1] & 0x0f) << 2]);
            ret.push_back(trailing_char);
         }
      }
      else
      {
         ret.push_back(base64_charset[(str[pos + 0] & 0x03) << 4]);
         ret.push_back(trailing_char);
         ret.push_back(trailing_char);
      }
   }

   LOGGER_LOG_TRACE("input {} with len {} has been encoded(64) into {} len {}", std::string(reinterpret_cast<const char *>(str)), len, ret, ret.length());

   return ret;
}


std::string BaseEncoder::base64Decode(const std::string& s) noexcept(true)
{
    if (s.empty())
      return std::string("");

   std::string ret;
   ret.reserve(s.length() / 4 * 3);

   for (size_t pos = 0; pos < s.length(); pos += 4)
   {
      const size_t base64CharPos_1 = base64CharPos(static_cast<unsigned char>(s[pos + 1]));
      ret.push_back(static_cast<std::string::value_type>(((base64CharPos(static_cast<unsigned char>(s[pos]))) << 2) + ((base64CharPos_1 & 0x30) >> 4)));

      if ((pos + 2 < s.length()) && s[pos + 2] != '=')
      {
         const size_t base64CharPos_2 = base64CharPos(static_cast<unsigned char>(s[pos + 2]));
         ret.push_back(static_cast<std::string::value_type>(((base64CharPos_1 & 0x0f) << 4) + ((base64CharPos_2 & 0x3c) >> 2)));

         if ((pos + 3 < s.length()) && s[pos + 3] != '=')
            ret.push_back(static_cast<std::string::value_type>(((base64CharPos_2 & 0x03) << 6 ) + base64CharPos(static_cast<unsigned char>(s[pos + 3]))));
      }
   }

   LOGGER_LOG_TRACE("input {} with len {} has been decoded(64) into {} len {}", s, s.length(), ret, ret.length());

   return ret;
}
