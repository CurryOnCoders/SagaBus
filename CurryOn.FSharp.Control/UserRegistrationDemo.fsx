#r "bin/debug/CurryOn.FSharp.Control.dll"

open FSharp.Control
open System
open System.Net.Mail
open System.Security.Cryptography
open System.Text
open System.Text.RegularExpressions

/// Mock database
type UserDb () =
    static member Query<'a> (query: string) = async { return [Unchecked.defaultof<'a>] }
    static member Find<'a> (key: string) = async { return if key = "Already Exists" then Some Unchecked.defaultof<'a> else Option<'a>.None }
    static member Insert<'a> (record: 'a) = async { return record }

/// Input from web form
type UserRegistrationRequest =
    {
        DesiredUserName: string
        DesiredPassword: string
        FirstName: string
        LastName: string
        EmailAddress: string
        PhoneNumber: string
        DateOfBirth: DateTime
    }

/// Registered User
type User = 
    { 
        UserName: string
        PasswordHash: byte []
        FirstName: string
        LastName: string
        EmailAddress: MailAddress
        PhoneNumber: string
        DateOfBirth: DateTime
        DateRegistered: DateTime
    }

/// Password validation criteria
type PasswordValidationError =
| TooShort of int*int
| MustContainUpperAndLowercase
| MustContainAtLeastOneNumber

/// Possible results of user registration process
type UserRegistrationEvents =
| UserNameIsBlank
| UserNameAlreadyTaken
| PasswordIsBlank
| PasswordDoesNotMeetCriteria of PasswordValidationError list
| FirstNameIsBlank
| LastNameIsBlank
| EmailAddressIsBlank
| EmailAddressIsNotValid
| PhoneNumberIsBlank
| PhoneNumberIsNotValid
| DateOfBirthAfterCutoff of DateTime*DateTime
| DatabaseAccessError of string
| UserRegisteredSuccessfully
| UnknownError of exn

/// Utility function for checking empty strings
let isEmpty = String.IsNullOrWhiteSpace

/// Regex for validation of Phone Numbers
let PhoneRegex = Regex "^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$"

/// Utility function to SHA-1 hash the password
let hashPassword = 
    let sha1 = SHA1.Create()
    fun (password: string) -> password |> Encoding.UTF8.GetBytes |> sha1.ComputeHash


/// Validate that the user name is not blank and is not already taken
let validateUserName userName =
    operation {
        if userName |> isEmpty
        then return! Result.failure [UserNameIsBlank]
        else try 
                 let! existingUser = UserDb.Find<User>(userName)
                 match existingUser with
                 | Some user -> return! Result.failure [UserNameAlreadyTaken]
                 | None -> return! Result.success userName
             with | ex ->
                return! Result.failure [DatabaseAccessError ex.Message]
    }

/// Validate that the password meets the criteria
let validatePassword password =
    if password |> isEmpty
    then Result.failure [PasswordIsBlank]
    else let validationErrors =
            [if password |> Seq.forall (Char.IsDigit >> not)
             then yield MustContainAtLeastOneNumber
             let characters = password |> Seq.filter Char.IsLetter in
             if (characters |> Seq.forall Char.IsLower) || (characters |> Seq.forall Char.IsUpper)
             then yield MustContainUpperAndLowercase
             if password.Length < 8
             then yield TooShort (password.Length, 8)]
         match validationErrors with
         | [] -> Result.success (hashPassword password)
         | errors -> Result.failure [PasswordDoesNotMeetCriteria errors]

/// Validate that the user has provided their first and last names
let validateName firstName lastName =
    let validationErrors = 
        [if firstName |> isEmpty
         then yield FirstNameIsBlank
         if lastName |> isEmpty
         then yield LastNameIsBlank]
    match validationErrors with
    | [] -> Result.success (firstName, lastName)
    | errors -> Result.failure errors

/// Validate Email Address
let validateEmail email =
    try MailAddress(email) |> Result.success
    with | :? FormatException -> Result.failure [EmailAddressIsNotValid]
         | _ -> Result.failure [EmailAddressIsBlank]
       
/// Validate Phone Number
let validatePhone phoneNumber =
    if phoneNumber |> isEmpty
    then Result.failure [PhoneNumberIsBlank]
    elif PhoneRegex.IsMatch phoneNumber
    then Result.success phoneNumber
    else Result.failure [PhoneNumberIsNotValid]

/// Validate That the User is Over 18
let validateDateOfBirth dateOfBirth =
    let cutoffDate = DateTime.Now.AddYears(-18)
    if dateOfBirth < cutoffDate
    then Result.success dateOfBirth
    else Result.failure [DateOfBirthAfterCutoff (dateOfBirth,cutoffDate)]

/// Attempt to register a new user in the UserDb based on a User Registration form
let registerUser (request: UserRegistrationRequest) =
    operation {
        let! userName = validateUserName request.DesiredUserName
        let! passwordHash = validatePassword request.DesiredPassword
        let! firstName, lastName = validateName request.FirstName request.LastName
        let! emailAddress = validateEmail request.EmailAddress
        let! phoneNumber = validatePhone request.PhoneNumber
        let! dateOfBirth = validateDateOfBirth request.DateOfBirth
        // If we make it this far, all validations have passed
        let newUser = { UserName = userName
                        PasswordHash = passwordHash
                        FirstName = firstName
                        LastName = lastName
                        EmailAddress = emailAddress
                        PhoneNumber = phoneNumber
                        DateOfBirth = dateOfBirth
                        DateRegistered = DateTime.Now
                      }
        try 
            let! registeredUser = UserDb.Insert<User> newUser 
            return! Result.success registeredUser
        with | ex -> 
            return! Result.failure [DatabaseAccessError ex.Message]

    }


// Examples:

registerUser 
    {DesiredUserName = "AaronE"; 
     DesiredPassword = "i like to type lots of characters"; 
     FirstName = "Aaron"; LastName = "Eshbach"; 
     EmailAddress = "aeshbach@sample.com"; 
     PhoneNumber = "555-236-8001"; 
     DateOfBirth = DateTime.Parse("12/31/1986")}
     |> Operation.wait


registerUser 
    {DesiredUserName = "APalmer"; 
     DesiredPassword = "S3asoned Wh1tebo4rd"; 
     FirstName = "Aaron"; LastName = "Palmer"; 
     EmailAddress = "apalmer@sample.com"; 
     PhoneNumber = "555-236-8001"; 
     DateOfBirth = DateTime.Parse("6/4/1944")}
     |> Operation.wait

registerUser 
    {DesiredUserName = "Already Exists"; 
     DesiredPassword = "f23890 qsSGdyt9"; 
     FirstName = "Already"; LastName = "Exists"; 
     EmailAddress = "aexists@sample.com"; 
     PhoneNumber = "555-236-8001"; 
     DateOfBirth = DateTime.Parse("1/1/1980")}
     |> Operation.wait

registerUser 
    {DesiredUserName = "c00lM4n"; 
     DesiredPassword = "1'M t3h b3st!"; 
     FirstName = "Leet"; LastName = "Haxor"; 
     EmailAddress = "fake@sample.com"; 
     PhoneNumber = "555-123-4567"; 
     DateOfBirth = DateTime.Parse("5/21/2003")}
     |> Operation.wait